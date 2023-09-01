# MapReduce

A MapReduce implementation for word frequency analysis in a large text file.

## How to Run
```sh
make
srun -N<NODES> -c<CPUS> ./map_reduce JOB_NAME NUM_REDUCER DELAY INPUT_FILENAME CHUNK_SIZE LOCALITY_CONFIG_FILENAME OUTPUT_DIR */
```

## Input
```input.txt
sponger algalia sponger algalia sponger statoblast
```

## Output
```
algalia 2
sponger 3
statoblast 1
```