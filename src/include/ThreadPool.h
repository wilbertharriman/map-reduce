//
// Created by pp22s48 on 12/14/22.
//

#ifndef MAP_REDUCE_THREADPOOL_H
#define MAP_REDUCE_THREADPOOL_H

#include <pthread.h>
#include <queue>

namespace MapReduce
{
    class ThreadPool;
    class ThreadPoolTask;

    class ThreadPoolTask {
        friend class ThreadPool;
    public:
        ThreadPoolTask(void* (*f)(void*), void* arg):f(f), arg(arg) {}
    private:
        void* (*f)(void*);
        void* arg;
    };

    class ThreadPool {
    public:
        ThreadPool(int numThreads) : _numThreads(numThreads) {
            _threads = new pthread_t[_numThreads];
            _terminate = false;

            pthread_mutex_init(&_mutex, nullptr);
            pthread_cond_init(&_addCond, nullptr);
            pthread_cond_init(&_removeCond, nullptr);
        }

        ~ThreadPool() {
            pthread_cond_destroy(&_addCond);
            pthread_cond_destroy(&_removeCond);
            pthread_mutex_destroy(&_mutex);

            while (!_tasks.empty()) {
                auto task = _tasks.front();
                _tasks.pop();

                delete task;
            }
            delete[] _threads;
        }

        bool getTerminate() const {
            return _terminate;
        }

        static void* run(void* arg);
        ThreadPoolTask* removeTask();
        void addTask(ThreadPoolTask* task);
        void start();
        void join();
        void terminate();

    private:
        int _numThreads;
        bool _terminate;
        const size_t _bufferSize = 10;

        pthread_t* _threads;
        pthread_mutex_t _mutex;
        pthread_cond_t _removeCond;
        pthread_cond_t _addCond;
        std::queue<ThreadPoolTask*> _tasks;
    };
}

#endif //MAP_REDUCE_THREADPOOL_H
