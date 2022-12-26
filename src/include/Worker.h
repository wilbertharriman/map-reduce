//
// Created by Wilbert Harriman on 12/13/22.
//

#ifndef MAP_REDUCE_WORKER_H
#define MAP_REDUCE_WORKER_H

#include <mpi.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include "ThreadPool.h"

namespace MapReduce {
    class Worker {
//    n_cpus - 1 mappers
//    1 reducer
//    Delay for D seconds to simulate remote read
    public:
        Worker(const int num_reducer, const int network_delay, const char* input_filename, const int chunk_size, const int worker_id, const int scheduler_id);
        void start();

    private:
        class MapperTask {
        public:
            MapperTask(Worker* worker, int chunk_id) : worker(worker), chunk_id(chunk_id) {}
            Worker *worker;
            int chunk_id;
        };
        static void* mapTask(void* arg);
        void mapTask(const int chunk_id);
        void reduceTask();
        void inputSplit(std::vector<std::string>& records , const int chunk_id);
        void map(std::vector<std::string>& records, std::unordered_map<std::string, int>& word_count);
        size_t partition(const std::string& word);

        void reduce();

        void remote_read_delay();
        int num_reducer;
        int network_delay;
        const char* input_filename;
        int chunk_size;
        ThreadPool *pool;

        int num_threads;
        int worker_id;
        int scheduler;
//        std::unordered_map<std::string, int> word_count;

        const int DONE = 0;
        const int JOB = 1;
    };
}

#endif //MAP_REDUCE_WORKER_H
