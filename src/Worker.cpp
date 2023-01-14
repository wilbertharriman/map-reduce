//
// Created by Wilbert Harriman on 12/13/22.
//

#include "include/Worker.h"

MapReduce::Worker::Worker(const char* job_name, const int num_reducer, const int network_delay, const char* input_filename, const int chunk_size, const int worker_id, const int scheduler_id, const int num_workers, const char* output_dir) {
    this->job_name = job_name;
    this->num_reducer = num_reducer;
    this->network_delay = network_delay;
    this->input_filename = input_filename;
    this->chunk_size = chunk_size;
    cpu_set_t cpuSet;
    sched_getaffinity(0, sizeof(cpuSet), &cpuSet);
    this->num_threads = CPU_COUNT(&cpuSet) - 1;
    this->worker_id = worker_id;
    this->scheduler = scheduler_id;
    this->num_workers = num_workers;
    this->output_dir = output_dir;

    this->pool = new ThreadPool(num_threads);
    this->pool->start();
}

void MapReduce::Worker::setTaskComplete(int task_id) {
    tasks[task_id] = 1;
}

void MapReduce::Worker::start() {
    MPI_Bcast(&num_chunks, 1, MPI_INT, scheduler, MPI_COMM_WORLD);

    this->tasks = new int[num_chunks + 1];

    for (int i = 0; i <= num_chunks; ++i) {
        tasks[i] = -1;
    }

    bool done = false;
    while (!done) {
        int finished_task = -1;

        // Report back any finish task
        for (int i = 1; i <= num_chunks; ++i) {
            if (tasks[i] == 1) {
                tasks[i] = -1;
                finished_task = i;
                break;
            }
        }

        MPI_Send(&finished_task, 1, MPI_INT, scheduler, 0, MPI_COMM_WORLD);

        int message[3];
        MPI_Recv(message, 3, MPI_INT, scheduler, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        done = message[0] == DONE;

        if (done) {
            // Acknowledge tasks complete
            bool tasks_not_done = true;
            while (tasks_not_done) {
                tasks_not_done = false;
                for (int task_num = 1; task_num <= num_chunks; ++task_num) {
                    if (tasks[task_num] == 1) {
                        tasks[task_num] = -1;
                        MPI_Request req;
                        MPI_Isend(&task_num, 1, MPI_INT, scheduler, 0, MPI_COMM_WORLD, &req);
                    } else if (tasks[task_num] == 0) {
                        tasks_not_done = true;
                    }
                } 
            }
            break;
        }

        int chunk_id = message[1];
        int node_id = message[2];

        tasks[chunk_id] = 0;

        // Reassemble a mapper task
        MapperTask *arg = new MapperTask(this, chunk_id, node_id);
        pool->addTask(new ThreadPoolTask(MapReduce::Worker::mapTask, static_cast<void *>(arg)));
    }
    pool->terminate();
    pool->join();

    MPI_Barrier(MPI_COMM_WORLD);

    bool reducer_done = false;

    while (!reducer_done) {
        int request = 2;
        MPI_Send(&request, 1, MPI_INT, scheduler, 0, MPI_COMM_WORLD);

        int message[3];
        MPI_Recv(message, 3, MPI_INT, scheduler, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        reducer_done = message[0] == DONE;

        if (reducer_done) {
            // Acknowledge task complete
            MPI_Request req;
            MPI_Isend(&request, 1, MPI_INT, scheduler, 0, MPI_COMM_WORLD, &req);
            break;
        }

        int task_id = message[1];

        reduceTask(task_id);
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

bool compare(std::pair<std::string, int>& a, std::pair<std::string, int>& b) {
    return a.first > b.first;
}

void MapReduce::Worker::sortWords(std::vector<std::pair<std::string, int>>& word_count) {
    sort(word_count.begin(), word_count.end(), [](std::pair<std::string, int>& a, std::pair<std::string, int>& b) {
        return a.first < b.first;
    });
}

void MapReduce::Worker::reduceTask(const int task_num) {
    std::vector<std::pair<std::string, int>> word_count;
    std::vector<std::pair<std::string, int>> word_total;

    // read part-task_num
    std::stringstream ss;
    const std::string FILENAME = "part";
    ss << output_dir << "/" << FILENAME << "-" << task_num <<  ".txt";

    std::ifstream intermediate_file(ss.str());

    std::string word;
    int count;
    while (intermediate_file >> word >> count) {
        word_count.push_back({word, count});
    }

    sortWords(word_count);

    intermediate_file.close();
    // remove intermediate file
    std::remove(ss.str().c_str());

    reduce(word_count, word_total);
    writeToFile(task_num, word_total);
}

void MapReduce::Worker::group(std::vector<std::pair<std::string, int>>::iterator& it,
                              std::vector<std::pair<std::string, int>>& word_count,
                              std::vector<std::pair<std::string, int>>& word_total) {
    std::string word = (*it).first;
    int total = (*it).second;
    ++it;
    while ((*it).first == word && it != word_count.end()) {
        total += (*it).second;
        ++it;
    }
    // append to word_total
    word_total.push_back({word, total});
}

void MapReduce::Worker::reduce(std::vector<std::pair<std::string, int>>& word_count, std::vector<std::pair<std::string, int>>& word_total) {
    for (auto it = word_count.begin(); it != word_count.end();) {
        group(it, word_count, word_total);
    }
}

void MapReduce::Worker::writeToFile(const int task_num, const std::vector<std::pair<std::string, int>>& word_total) {
    std::stringstream ss;
    ss << output_dir << "/" << job_name << "-" << task_num << ".out";

    std::ofstream outfile(ss.str());
    for (const auto word_total_pair : word_total) {
        const std::string word = word_total_pair.first;
        int total = word_total_pair.second;
        outfile << word << " " << total << std::endl;
    }
    outfile.close();
}

void MapReduce::Worker::inputSplit(std::vector<std::string>& records, const int chunk_id) {
    int line_offset = (chunk_id - 1) * chunk_size;
    std::ifstream input_file(this->input_filename);
    std::string line;

    int line_num = 0;
    while (line_num < line_offset && getline(input_file, line)) {
        ++line_num;
    }

    for (int i = line_offset; i < line_offset + chunk_size; ++i) {
        getline(input_file, line);
        records.push_back(line);
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
    return std::hash<std::string>{}(word) % num_reducer;
}

void* MapReduce::Worker::mapTask(void* arg) {
    MapperTask* task = static_cast<MapperTask *>(arg);

    int chunk_id = task->chunk_id;
    int chunk_location = task->node_id;
    Worker *worker = task->worker;

    // remote read
    if (chunk_location != worker->worker_id) {
        // std::cout << "Fetching data chunk from node " << chunk_location << " to node " << worker->worker_id << " for task " << chunk_id << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(worker->network_delay));
    }
    int num_reducer = worker->num_reducer;

    std::vector<std::string> records;
    std::unordered_map<std::string, int> word_count;

    worker->inputSplit(records, chunk_id);
    worker->map(records, word_count);

    std::stringstream ss;
    const std::string FILENAME = "tmp";
    ss << worker->output_dir << "/" << FILENAME << "-" << chunk_id << ".txt";
    std::ofstream outfile;
    outfile.open(ss.str());
    for (auto it = word_count.begin(); it != word_count.end(); ++it) {
        outfile << (*it).first << " " << (*it).second << std::endl;
    }
    outfile.close();

    worker->setTaskComplete(chunk_id);
    return nullptr;
}