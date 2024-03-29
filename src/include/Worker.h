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
#include <map>
#include <thread>
#include <chrono>
#include <algorithm>
#include "ThreadPool.h"

namespace MapReduce {
    class Worker {
    public:
        Worker(const char* job_name, const int num_reducer, const int network_delay, const char* input_filename, const int chunk_size, const int worker_id, const int scheduler_id, const int num_workers, const char* output_dir);
        void start();

    private:
        class MapperTask {
        public:
            MapperTask(Worker* worker, int chunk_id, int node_id) :
                worker(worker),
                chunk_id(chunk_id),
                node_id(node_id){}
            Worker *worker;
            int chunk_id;
            int node_id;
        };
        static void* mapTask(void* arg);
        void reduceTask(const int task_num);
        void inputSplit(std::vector<std::string>& records , const int chunk_id);
        void setTaskComplete(int chunk_id);
        void map(std::vector<std::string>& records, std::unordered_map<std::string, int>& word_count);
        size_t partition(const std::string& word);
        void writeToFile(const int task_num, const std::vector<std::pair<std::string, int>> &word_total);


        void sortWords(std::vector<std::pair<std::string, int>>& word_count);
        void group(std::vector<std::pair<std::string, int>>::iterator& it,
                   std::vector<std::pair<std::string, int>>& word_count,
                   std::vector<std::pair<std::string, int>>& word_total);

        void reduce(std::vector<std::pair<std::string, int>>& word_count, std::vector<std::pair<std::string, int>>& word_total);

        void remote_read_delay();
        std::string job_name;
        int num_reducer;
        int network_delay;
        const char* input_filename;
        int chunk_size;
        int num_chunks;
        const char* output_dir;
        ThreadPool *pool;

        int num_threads;
        int worker_id;
        int scheduler;
        int num_workers;
        int *tasks;

        const int DONE = 0;
        const int JOB = 1;
    };
}

#endif //MAP_REDUCE_WORKER_H
