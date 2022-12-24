//
// Created by Wilbert Harriman on 12/13/22.
//

#include "include/Worker.h"

MapReduce::Worker::Worker(const int network_delay, const char* input_filename, const int chunk_size, const int worker_id, const int scheduler_id) {
    this->network_delay = network_delay;
    this->input_filename = input_filename;
    this->chunk_size = chunk_size;
    cpu_set_t cpuSet;
    sched_getaffinity(0, sizeof(cpuSet), &cpuSet);
    this->num_threads = CPU_COUNT(&cpuSet);
    this->pool = new ThreadPool(num_threads);
    this->worker_id = worker_id;
    this->scheduler = scheduler_id;
}

void MapReduce::Worker::start() {
    // define
    // 0: kill signal
    // 1: map task
    // 2: reduce task

    bool killed = false;
    while (!killed) {
        int request_type = 1;
        MPI_Send(&request_type, 1, MPI_INT, scheduler, 0, MPI_COMM_WORLD);

        int message[2];
        MPI_Recv(message, 2, MPI_INT, scheduler, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        killed = message[0] == 0;
        int chunk_id = message[1];
        // DEBUG
        if (!killed)
            std::cout << "worker " << this->worker_id << " received chunk " << chunk_id << std::endl;
    }
}

void MapReduce::Worker::inputSplit(const int chunk_id) {
    int line_offset = chunk_id * chunk_size;
    std::ifstream input_file(this->input_filename);
    std::string line;

    int line_num = 0;
    while (line_num < line_offset && getline(input_file, line)) {
        ++line_num;
    }

    for (int i = 0; i < chunk_size; ++i) {
        getline(input_file, line);
        std::cout << line << std::endl;
    }

    input_file.close();
}

void MapReduce::Worker::map() {
//    inputSplit(chunk_id);
    // input split
    // map
    // partition
}

void MapReduce::Worker::reduce() {
    // sort
    // group
    // reduce
    // outpu
}
