from app.mapper import Mapper 
from app.reducer import Reducer

import multiprocessing
import time

def main():

    # Define number of mapper and reducer processes
    num_mapper_processes = 5
    num_reducer_processes = 3

    # Define the data directories
    data = ["app/data_dir1", "app/data_dir2", "app/data_dir3", "app/data_dir4", "app/data_dir5"]

    # Create a queue associated with each reducer 
    reducer_queues = [multiprocessing.Queue() for _ in range(num_reducer_processes)]

    # Create and start reducer processes
    print("Start a pool of reducers")
    reducer_processes = []
    for i in range(num_reducer_processes):
        reducer = Reducer(i, num_mapper_processes, reducer_queues[i])
        reduce_process = multiprocessing.Process(target=reducer.reduce)
        reducer_processes.append(reduce_process)
        reduce_process.start()
    
    print("Waiting for 4 seconds...")
    time.sleep(4)
    print("Done waiting!")
    time.sleep(2)

    # Create and start mapper processes
    print("Create mappers")
    time.sleep(2)
    mapper_processes = []
    for i in range(num_mapper_processes):
        data_directory = data[i % len(data)]
        mapper = Mapper(reducer_queues, num_reducer_processes, data_directory)
        map_process = multiprocessing.Process(target=mapper.map)
        mapper_processes.append(map_process)
    
    print("Waiting for 4 seconds...")
    print("Starting mapper proccesses")
    time.sleep(4)
    for map_process in mapper_processes:
        map_process.start()

    # Wait for mapper processes to finish
    for process in mapper_processes:
        process.join()
    
    # Wait for reducer processes to finish
    for process in reducer_processes:
        process.join()

    print("DONE: all mappers and reducers terminated")
    return

if __name__ == "__main__":
    main()