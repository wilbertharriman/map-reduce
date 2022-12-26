//
// Created by Wilbert Harriman on 12/13/22.
//

#include "include/Worker.h"

MapReduce::Worker::Worker(const int num_reducer, const int network_delay, const char* input_filename, const int chunk_size, const int worker_id, const int scheduler_id) {
    this->num_reducer = num_reducer;
    this->network_delay = network_delay;
    this->input_filename = input_filename;
    this->chunk_size = chunk_size;
    cpu_set_t cpuSet;
    sched_getaffinity(0, sizeof(cpuSet), &cpuSet);
    this->num_threads = CPU_COUNT(&cpuSet) - 1;
    this->pool = new ThreadPool(num_threads);
    this->pool->start();
    this->worker_id = worker_id;
    this->scheduler = scheduler_id;
//    this->word_count.resize(num_reducer);
}

void MapReduce::Worker::start() {
    // define
    // 0: done signal
    // 1: map task

    bool done = false;
    while (!done) {
        int request_type = 1;
        MPI_Send(&request_type, 1, MPI_INT, scheduler, 0, MPI_COMM_WORLD);

        int message[2];
        MPI_Recv(message, 2, MPI_INT, scheduler, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        done = message[0] == DONE;
        int chunk_id = message[1];

        // Reassemble a mapper task
        MapperTask *arg = new MapperTask(this, chunk_id);
        pool->addTask(new ThreadPoolTask(MapReduce::Worker::mapTask, static_cast<void *>(arg)));

        // DEBUG
//        if (!done)
//            std::cout << "worker " << this->worker_id << " received chunk " << chunk_id << std::endl;
    }
    pool->terminate();
    pool->join();
    // Reduce
    // distribute reducer tasks
    // reduce
}

void MapReduce::Worker::inputSplit(std::vector<std::string>& records, const int chunk_id) {
    int line_offset = chunk_id * chunk_size;
    std::ifstream input_file(this->input_filename);
    std::string line;

    int line_num = 0;
    while (line_num < line_offset && getline(input_file, line)) {
        ++line_num;
    }

    for (int i = line_offset; i < line_offset + chunk_size; ++i) {
        getline(input_file, line);
        records.push_back(line);
        // DEBUG
//        std::cout << line << std::endl;
    }

    input_file.close();
}

void MapReduce::Worker::map(std::vector<std::string>& records, std::unordered_map<std::string, int>& word_count) {
    for (auto line : records) {
        std::stringstream words(line);
        std::string word;
        while (words >> word) {
            word_count[word] = word_count[word] + 1;
        }
    }
}

size_t MapReduce::Worker::partition(const std::string& word) {
    return std::hash<std::string>()(word) % num_reducer;
}

void* MapReduce::Worker::mapTask(void* arg) {
    MapperTask* task = static_cast<MapperTask *>(arg);
    int chunk_id = task->chunk_id;
    Worker *worker = task->worker;

    std::vector<std::string> records;
    std::unordered_map<std::string, int> word_count;

    worker->inputSplit(records, chunk_id);
    worker->map(records, word_count);
    // partition

    // write to file
    std::stringstream ss;
    const std::string FILENAME = "tmp";
    ss << FILENAME << "-" << chunk_id <<  ".txt";

    std::ofstream outfile;
    outfile.open(ss.str());
    for (auto it = word_count.begin(); it != word_count.end(); ++it) {
        outfile << (*it).first << " " << (*it).second << std::endl;
    }
    outfile.close();

    return nullptr;
}