//
// Created by Wilbert Harriman on 12/13/22.
//

#ifndef MAP_REDUCE_WORKER_H
#define MAP_REDUCE_WORKER_H

#include <mpi.h>
#include <iostream>
#include <fstream>
#include "ThreadPool.h"

namespace MapReduce {
    class Worker {
//    n_cpus - 1 mappers
//    1 reducer
//    Delay for D seconds to simulate remote read
    public:
        Worker(const int network_delay, const char* input_filename, const int chunk_size, const int worker_id, const int scheduler_id);
        void start();

    private:
        void map();
        void reduce();
        void remote_read_delay();

        int network_delay;
        const char* input_filename;
        int chunk_size;
        ThreadPool *pool;
        int num_threads;

        void inputSplit(const int chunk_id);
        int worker_id;
        int scheduler;
    };
}

#endif //MAP_REDUCE_WORKER_H
