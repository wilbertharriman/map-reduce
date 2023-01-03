#include "include/Scheduler.h"
#include "include/Worker.h"

using namespace MapReduce;

int main(int argc, char** argv) {
    /* srun -N<NODES> -c<CPUS> ./map-reduce JOB_NAME NUM_REDUCER DELAY
    INPUT_FILENAME CHUNK_SIZE LOCALITY_CONFIG_FILENAME OUTPUT_DIR */
    if (argc < 8) {
        throw std::invalid_argument("Not enough arguments!");
        return 1;
    }

    char* JOB_NAME = argv[1];
    int NUM_REDUCER = std::stoi(argv[2]);
    int NETWORK_DELAY = std::stoi(argv[3]);
    char* INPUT_FILENAME = argv[4];
    int CHUNK_SIZE = std::stoi(argv[5]);
    char* LOCALITY_CONFIG_FILENAME = argv[6];
    char* OUTPUT_DIR = argv[7];

    MPI_Init(&argc, &argv);

    int cluster_size;
    MPI_Comm_size(MPI_COMM_WORLD, &cluster_size);

    int node_id;
    MPI_Comm_rank(MPI_COMM_WORLD, &node_id);

    if (node_id == cluster_size - 1) {
        // Assign the last node in the cluster as scheduler
        Scheduler *scheduler = new MapReduce::Scheduler(
                LOCALITY_CONFIG_FILENAME,
                cluster_size - 1,
                cluster_size - 1);
        scheduler->start();
    } else {
        Worker *worker = new MapReduce::Worker(
                JOB_NAME,
                NUM_REDUCER,
                NETWORK_DELAY,
                INPUT_FILENAME,
                CHUNK_SIZE,
                node_id,
                cluster_size - 1,
                cluster_size - 1);
        worker->start();
    }

    MPI_Finalize();
    return 0;
}
