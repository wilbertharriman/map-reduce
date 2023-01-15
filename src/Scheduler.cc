//
// Created by Wilbert Harriman on 12/13/22.
//

#include "include/Scheduler.h"

MapReduce::Scheduler::Scheduler(
    const char* job_name,
    int cluster_size,
    int num_reducer,
    int network_delay,
    const char* input_filename,
    int chunk_size,
    const char* locality_config_filename,
    const char* output_dir,
    int scheduler_id)
    :
    job_name(job_name),
    cluster_size(cluster_size),
    num_reducer(num_reducer),
    network_delay(network_delay),
    input_filename(input_filename),
    chunk_size(chunk_size),
    locality_config_filename(locality_config_filename),
    output_dir(output_dir),
    scheduler_id(scheduler_id) {

    cpu_set_t cpuSet;
    sched_getaffinity(0, sizeof(cpuSet), &cpuSet);
    this->cpu_cores = CPU_COUNT(&cpuSet);
    this->num_workers = cluster_size - 1;

    int status = mkdir(output_dir, 0777);

    std::stringstream ss;
    ss << output_dir << "/" << job_name;
    this->logger = new Logger(ss.str());
}

void MapReduce::Scheduler::start() {
    double start_time = MPI_Wtime();

    logger->log("Start_Job,%s,%d,%d,%d,%d,%s,%d,%s,%s", 
        job_name, 
        cluster_size, 
        cpu_cores, 
        num_reducer, 
        network_delay, 
        input_filename, 
        chunk_size, 
        locality_config_filename, 
        output_dir);
    createTasks();
    MPI_Bcast(&num_chunks, 1, MPI_INT, scheduler_id, MPI_COMM_WORLD);
    dispatchMapperTasks();
    // Wait for all mapper tasks to finish
    MPI_Barrier(MPI_COMM_WORLD);

    int total_kv_pair = analyzeIntermediateFiles();

    logger->log("Start_Shuffle,%d", total_kv_pair);
    auto now = std::chrono::system_clock::now();
    long shuffle_start_time = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    shuffle();
    now = std::chrono::system_clock::now();
    long shuffle_end_time = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();;
    long shuffle_duration = shuffle_end_time - shuffle_start_time; // ms
    logger->log("Finish_Shuffle,%d", shuffle_duration);

    dispatchReducerTasks();
    terminateWorkers();

    MPI_Barrier(MPI_COMM_WORLD);
    double end_time = MPI_Wtime();
    int duration = static_cast<int>(end_time - start_time);
    logger->log("Finish_Job,%d", duration);
}

size_t MapReduce::Scheduler::partition(const std::string& word) {
    return std::hash<std::string>{}(word) % num_reducer;
}

int MapReduce::Scheduler::analyzeIntermediateFiles() {
    const std::string FILENAME = "tmp";
    int total_key_value_pair = 0;
    for (int chunk_id = 1; chunk_id <= num_chunks; ++chunk_id) {
        std::stringstream ss;
        ss << output_dir << "/" << FILENAME << "-" << chunk_id <<  ".txt";

        std::ifstream intermediate_file(ss.str());

        std::string word;
        int count;
        while (intermediate_file >> word >> count) {
            ++total_key_value_pair;
        }

        intermediate_file.close();
    }
    return total_key_value_pair;
}

void MapReduce::Scheduler::shuffle() {
    std::vector<std::multimap<std::string, int>> word_count;
    word_count.resize(num_reducer);

    const std::string FILENAME = "tmp";
    for (int chunk_id = 1; chunk_id <= num_chunks; ++chunk_id) {
        std::stringstream ss;
        ss << output_dir << "/" << FILENAME << "-" << chunk_id <<  ".txt";

        std::ifstream intermediate_file(ss.str());

        std::string word;
        int count;
        while (intermediate_file >> word >> count) {
            size_t partition_id = partition(word);
            word_count[partition_id].insert({word, count});
        }

        intermediate_file.close();
        std::remove(ss.str().c_str());
    }

    for (int partition_id = 0; partition_id < num_reducer; ++partition_id) {
        // open a file
        std::stringstream ss;
        ss << output_dir << "/" << "part" << "-" << partition_id << ".txt";
        std::ofstream partition_file(ss.str());

        for (auto it = word_count[partition_id].begin(); it != word_count[partition_id].end(); ++it) {
            std::string word = it->first;
            int count = it->second;
            partition_file << word << " " << count << std::endl;
        }
        partition_file.close();
    }
}

void MapReduce::Scheduler::createTasks() {
    // open file
    std::ifstream locality_file(locality_config_filename);
    int chunk_id;
    int node_id;

    while (locality_file >> chunk_id >> node_id) {
        tasks.push_back(new MapperTask(node_id % num_workers, chunk_id));
    }

    this->num_chunks = tasks.size();

    locality_file.close();
}

void MapReduce::Scheduler::dispatchMapperTasks() {
    bool finished_tasks[num_chunks + 1];
    double task_start_time[num_chunks + 1] = {0};

    for (int i = 0; i <= num_chunks; ++i) {
        finished_tasks[i] = false;
    }

    while (!tasks.empty()) {
        int finished_task = -1;
        MPI_Status status;
        MPI_Recv(&finished_task, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        int worker_id = status.MPI_SOURCE;

        if (finished_task != -1) {
            double end_time = MPI_Wtime();
            int duration = static_cast<int>(end_time - task_start_time[finished_task]);
            logger->log("Complete_MapTask,%d,%d", finished_task, duration);

            finished_tasks[finished_task] = true;
            task_start_time[finished_task] = 0;
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
            task_start_time[task_to_dispatch->chunk_id] = MPI_Wtime();
 
            MPI_Isend(message, 3, MPI_INT, worker_id, 0, MPI_COMM_WORLD, &mpi_req);

            delete task_to_dispatch;
        }
    }
    terminateWorkers();

    // Wait ACK from workers
    bool tasks_not_done = true;
    while (tasks_not_done) {
        int finished_task = -1;
        MPI_Status status;
        MPI_Recv(&finished_task, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

        if (finished_task != -1) {
            int worker_id = status.MPI_SOURCE;
            double end_time = MPI_Wtime();
            int duration = static_cast<int>(end_time - task_start_time[finished_task]);
            logger->log("Complete_MapTask,%d,%d", finished_task, duration);

            finished_tasks[finished_task] = true;
            task_start_time[finished_task] = 0;
            tasks_not_done = false;
            for (int i = 1; i <= num_chunks; ++i) {
                if (finished_tasks[i] == false) {
                    tasks_not_done = true;
                }
            }
        }
    }
}

void MapReduce::Scheduler::dispatchReducerTasks() {
    int worker_task[num_workers];
    double worker_start_time[num_workers] = {0};

    for (int i = 0; i < num_workers; ++i) {
        worker_task[i] = -1;
    }

    for (int task_id = 0; task_id < num_reducer; ++task_id) {
        int finished_task;
        MPI_Status status;
        MPI_Recv(&finished_task, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

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
