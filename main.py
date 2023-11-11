from app.mapper import Mapper 
from app.reducer import Reducer

import multiprocessing
import time
import random

def main():

    # Define number of mapper and reducer processes
    num_mapper_processes = 5
    num_reducer_processes = 3

    # Define the data directories
    # data = ["app/data_dir1", "app/data_dir2", "app/data_dir3", "app/data_dir4", "app/data_dir5"]
    # Oscar Wilde The Portrait of Dorian Gray
    data = ["app/oscar_wilde_data/chapters1_4", "app/oscar_wilde_data/chapters5_8", "app/oscar_wilde_data/chapters9_12", "app/oscar_wilde_data/chapters13_16", "app/oscar_wilde_data/chapters17_20"]

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
    time.sleep(1)

    # Create and start mapper processes
    print("Create mappers and start mapper proccesses")
    time.sleep(4)
    mapper_processes = []
    for i in range(num_mapper_processes):
        data_directory = data[i % len(data)]
        mapper = Mapper(reducer_queues, data_directory)
        map_process = multiprocessing.Process(target=mapper.map)
        mapper_processes.append(map_process)
        map_process.start()
        ###
        # # Simulate a random process dying
        # if random.random() < 0.2: 
        #     print("Kill reducer process number", i)
        #     map_process.terminate()
        #     map_process.join()
        # # Simulate a random process dying
        # if random.random() < 0.2: 
        #     print("Kill reducer process number", len(reducer_processes)-1)
        #     reducer_processes[len(reducer_processes)-1].terminate()
        #     reducer_processes[len(reducer_processes)-1].join()
        ###


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