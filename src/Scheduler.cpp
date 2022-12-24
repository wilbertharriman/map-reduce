//
// Created by Wilbert Harriman on 12/13/22.
//

#include "include/Scheduler.h"

MapReduce::Scheduler::Scheduler(const char *locality_config_filename, const int num_workers) {
    this->locality_config_filename = locality_config_filename;
    this->num_workers = num_workers;
}

void MapReduce::Scheduler::start() {
    createTasks();
    dispatchTasks();
    terminateWorkers();
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

void MapReduce::Scheduler::createTasks() {
    // open file
    std::ifstream locality_file(locality_config_filename);
    int chunk_id;
    int node_id;

    // TODO: parallelize
    while (locality_file >> chunk_id >> node_id) {
        tasks.push_back(new MapperTask(node_id % num_workers, chunk_id));
    }
    std::cout << tasks.size() << " tasks are created" << std::endl;

    locality_file.close();
}

void MapReduce::Scheduler::dispatchTasks() {
    while (!tasks.empty()) {
        for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
            int request_type; // 1: mapper thread or 2: reducer thread
            MPI_Recv(&request_type, 1, MPI_INT, worker_id, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            MapperTask *task_to_dispatch = getTaskFor(worker_id);
            int message[2];

            if (task_to_dispatch == nullptr) {
                message[0] = KILL_SIGNAL;
                message[1] = 0;
                MPI_Send(message, 2, MPI_INT, worker_id, 0, MPI_COMM_WORLD);
            } else {
                message[0] = 1;
                message[1] = task_to_dispatch->chunk_id;
                MPI_Send(message, 2, MPI_INT, worker_id, 0, MPI_COMM_WORLD);
                delete task_to_dispatch;
            }
        }
    }
}

void MapReduce::Scheduler::terminateWorkers() {
    for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
        int message[2];
        message[0] = KILL_SIGNAL;
        message[1] = 0;
        MPI_Request request;

        MPI_Isend(message, 2, MPI_INT, worker_id, 0, MPI_COMM_WORLD, &request);
    }
}

