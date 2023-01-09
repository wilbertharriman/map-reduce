//
// Created by Wilbert Harriman on 12/13/22.
//

#ifndef MAP_REDUCE_SCHEDULER_H
#define MAP_REDUCE_SCHEDULER_H

#include <iostream>
#include <sys/stat.h>
#include <fstream>
#include <sstream>
#include <list>
#include <mpi.h>
#include "Logger.h"
#include "ThreadPool.h"

namespace MapReduce
{
    class Scheduler {

    public:
        Scheduler(
            const char* job_name,
            int cluster_size,
            int num_reducer,
            int delay,
            const char* input_filename,
            int chunk_size,
            const char* locality_config_filename,
            const char* output_dir,
            int scheduler_id);
        void start();
    private:
        class MapperTask {
        public:
            MapperTask(int node_id, int chunk_id) : node_id(node_id), chunk_id(chunk_id) {}
            int node_id;
            int chunk_id;
        };

        void createTasks();
        void dispatchTasks();
        void terminateWorkers();
        std::list<MapperTask*> tasks;

        MapperTask *getTaskFor(const int worker_id);
        const int DONE = 0;
        const int JOB = 1;

        Logger *logger;
        int num_workers;
        int num_chunks;
        int cpu_cores;

        const char* job_name;
        int cluster_size;
        int num_reducer;
        int network_delay;
        const char* input_filename;
        int chunk_size;
        const char* locality_config_filename;
        const char* output_dir;
        int scheduler_id;
    };
}

#endif //MAP_REDUCE_SCHEDULER_H
