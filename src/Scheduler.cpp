//
// Created by Wilbert Harriman on 12/13/22.
//

#include "include/Scheduler.h"

MapReduce::Scheduler::Scheduler(const std::string& job_name, const int num_reducer, const char *locality_config_filename, const int num_workers, const int scheduler_id) {
    this->locality_config_filename = locality_config_filename;
    this->num_reducer = num_reducer;
    this->num_workers = num_workers;
    this->id = scheduler_id;
    this->logger = new Logger(job_name);
}

void MapReduce::Scheduler::start() {
    logger->log("Start_Job");
    createTasks();
    dispatchTasks();

    MPI_Barrier(MPI_COMM_WORLD);
    logger->log("Finish_Job");
}

void MapReduce::Scheduler::createTasks() {
    // open file
    std::ifstream locality_file(locality_config_filename);
    int chunk_id;
    int node_id;

    while (locality_file >> chunk_id >> node_id) {
        // DEBUG
//        std::cout << num_workers << std::endl;
        tasks.push_back(new MapperTask(node_id % num_workers, chunk_id));
    }

    this->num_chunks = tasks.size();
    // DEBUG
//    std::cout << this->num_chunks << " tasks are created" << std::endl;

    locality_file.close();
}

void MapReduce::Scheduler::dispatchTasks() {
    int worker_task[num_workers];
    double worker_start_time[num_workers] = {0};

    for (int i = 0; i < num_workers; ++i) {
        worker_task[i] = -1;
    }

    while (!tasks.empty()) {
        int request; // 1: mapper thread or 2: reducer thread
        MPI_Status status;
        MPI_Recv(&request, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

        int worker_id = status.MPI_SOURCE;

        if (worker_task[worker_id] != -1) {
            double end_time = MPI_Wtime();
            int duration = static_cast<int>(end_time - worker_start_time[worker_id]);
            logger->log("Complete_MapTask,%d,%d", worker_task[worker_id], duration);
            worker_task[worker_id] = -1;
            worker_start_time[worker_id] = 0;
        }

        MapperTask *task_to_dispatch = getTaskFor(worker_id);
        int message[3];

        MPI_Request mpi_req;
        if (task_to_dispatch == nullptr) {
            message[0] = DONE;
            message[1] = DONE;
            message[2] = DONE;
            MPI_Isend(message, 3, MPI_INT, worker_id, 0, MPI_COMM_WORLD, &mpi_req);
        } else {
            message[0] = JOB;
            message[1] = task_to_dispatch->chunk_id;
            message[2] = task_to_dispatch->node_id;

            logger->log("Dispatch_MapTask,%d,%d", task_to_dispatch->chunk_id, worker_id);
            worker_task[worker_id] = task_to_dispatch->chunk_id;
            worker_start_time[worker_id] = MPI_Wtime();
            MPI_Isend(message, 3, MPI_INT, worker_id, 0, MPI_COMM_WORLD, &mpi_req);

            delete task_to_dispatch;
        }
    }
    for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
        if (worker_task[worker_id] != -1) {
            int ack;
            MPI_Recv(&ack, 1, MPI_INT, worker_id, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            double end_time = MPI_Wtime();
            int duration = static_cast<int>(end_time - worker_start_time[worker_id]);
            logger->log("Complete_MapTask,%d,%d", worker_task[worker_id], duration);

            worker_task[worker_id] = -1;
            worker_start_time[worker_id] = 0;

            int message[3];
            message[0] = DONE;
            message[1] = DONE;
            message[2] = DONE;
            
            MPI_Request request;
            MPI_Isend(message, 3, MPI_INT, worker_id, 0, MPI_COMM_WORLD, &request);
        }
    }

    MPI_Bcast(&num_chunks, 1, MPI_INT, id, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);

    for (int task_id = 0; task_id < num_reducer; ++task_id) {
        int request; // 1: mapper thread or 2: reducer thread
        MPI_Status status;
        MPI_Recv(&request, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

        int worker_id = status.MPI_SOURCE;

        if (worker_task[worker_id] != -1) {
            double end_time = MPI_Wtime();
            int duration = static_cast<int>(end_time - worker_start_time[worker_id]);
            logger->log("Complete_ReduceTask,%d,%d", worker_task[worker_id], duration);
            worker_task[worker_id] = -1;
            worker_start_time[worker_id] = 0;
        }

        int message[3];
        message[0] = JOB;
        message[1] = task_id;
        message[2] = task_id;

        logger->log("Dispatch_ReduceTask,%d,%d", task_id, worker_id);
        worker_task[worker_id] = task_id;
        worker_start_time[worker_id] = MPI_Wtime();

        MPI_Request mpi_req;
        MPI_Isend(message, 3, MPI_INT, worker_id, 0, MPI_COMM_WORLD, &mpi_req);
    }

    for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
        if (worker_task[worker_id] != -1) {
            int ack;
            MPI_Recv(&ack, 1, MPI_INT, worker_id, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            double end_time = MPI_Wtime();
            int duration = static_cast<int>(end_time - worker_start_time[worker_id]);
            logger->log("Complete_ReduceTask,%d,%d", worker_task[worker_id], duration);

            worker_task[worker_id] = -1;
            worker_start_time[worker_id] = 0;

            int message[3];
            message[0] = DONE;
            message[1] = DONE;
            message[2] = DONE;
            
            MPI_Request request;
            MPI_Isend(message, 3, MPI_INT, worker_id, 0, MPI_COMM_WORLD, &request);
        }
    }

    terminateWorkers();
}

void MapReduce::Scheduler::terminateWorkers() {
    for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
        int message[3];
        message[0] = DONE;
        message[1] = DONE;
        message[2] = DONE;
        MPI_Request request;

        MPI_Isend(message, 3, MPI_INT, worker_id, 0, MPI_COMM_WORLD, &request);
    }
}

MapReduce::Scheduler::MapperTask* MapReduce::Scheduler::getTaskFor(const int worker_id) {
    if (tasks.empty())
        return nullptr;

    for (auto it = tasks.begin(); it != tasks.end(); ++it) {
        if ((*it)->node_id == worker_id) {
            MapperTask *task = *it;
            tasks.erase(it);
            return task;
        }
    }

    MapperTask* task = tasks.front();
    tasks.pop_front();
    return task;
}
