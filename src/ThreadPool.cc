//
// Created by Wilbert Harriman on 12/14/22.
//

#include "include/ThreadPool.h"
#include <iostream>

void *MapReduce::ThreadPool::run(void *args) {
    auto *pool = (ThreadPool*)args;

    while (!pool->_terminate || !pool->_tasks.empty()) {
        ThreadPoolTask* task = pool->removeTask();

        if (task != nullptr) {
            (*(task->f))(task->arg);
            delete task;
        }
    }
    return nullptr;
}

MapReduce::ThreadPoolTask *MapReduce::ThreadPool::removeTask() {
    pthread_mutex_lock(&_mutex);

    while (_tasks.empty() and !_terminate) {
        pthread_cond_wait(&_removeCond, &_mutex);
    }

    if (_terminate and _tasks.empty()) {
        pthread_mutex_unlock(&_mutex);
        return nullptr;
    }

    ThreadPoolTask* nextTask = _tasks.front();
    _tasks.pop();
    pthread_cond_signal(&_addCond);

    pthread_mutex_unlock(&_mutex);

    return nextTask;
}

void MapReduce::ThreadPool::addTask(MapReduce::ThreadPoolTask *task) {
    pthread_mutex_lock(&_mutex);

    while (_tasks.size() >= _bufferSize and !_terminate) {
        pthread_cond_wait(&_addCond, &_mutex);
    }

    if (_terminate) {
        pthread_mutex_unlock(&_mutex);
        return;
    }
    _tasks.push(task);
    pthread_cond_signal(&_removeCond);
    pthread_mutex_unlock(&_mutex);
}

void MapReduce::ThreadPool::start() {
    for (int i = 0; i < _numThreads; ++i) {
        pthread_create(&_threads[i], 0, ThreadPool::run, (void*)this);
    }
}

void MapReduce::ThreadPool::join() {
    for (int i = 0; i < _numThreads; ++i) {
        pthread_join(_threads[i], nullptr);
    }
}

void MapReduce::ThreadPool::terminate() {
    pthread_mutex_lock(&_mutex);

    _terminate = true;
    pthread_cond_broadcast(&_addCond);
    pthread_cond_broadcast(&_removeCond);
    pthread_mutex_unlock(&_mutex);
}