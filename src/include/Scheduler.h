//
// Created by Wilbert Harriman on 12/13/22.
//

#ifndef MAP_REDUCE_SCHEDULER_H
#define MAP_REDUCE_SCHEDULER_H

#include <iostream>
#include <fstream>
#include <list>
#include <mpi.h>
#include <memory>
#include "ThreadPool.h"

namespace MapReduce
{
    class Scheduler {

    public:
//        TODO: LOGGER
        Scheduler(const char *locality_config_filename, int num_workers);
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
        const char *locality_config_filename;
        int num_workers;

        MapperTask *getTaskFor(const int worker_id);
        const int DONE = 0;
        const int JOB = 1;
    };
}

#endif //MAP_REDUCE_SCHEDULER_H
