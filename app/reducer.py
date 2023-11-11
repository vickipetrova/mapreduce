class Reducer:
    """ 
    A reducer class to perform the reduce stage of MapReduce 

    Attributes
    ----------
    reducer_id: int
        The id associated with each reducer
    num_map_processes: int
        The number of mapping processes the reducer should know about
    input_queue: Queue
        The queue associated with the reducer
    word_storage: dictionary
        Used to temporarily store the word-count pairs before terminating and writing them to file

    Methods
    -------
    reduce:
        Once the reduce process is started, it waits for any input to be sent down its queue and proccesses it. Once all mappers terminate and send EOF, the reducer writes its output to a file in the output folder. 
    """

    def __init__(self, reducer_id, num_map_processes, input_queue):
        self.reducer_id = reducer_id
        self.num_map_processes = num_map_processes
        self.input_queue = input_queue

        self.word_storage = {}

    def reduce(self):
        # A print statement for observability.
        print("⏩️ REDUCE operation initialized - id:", self.reducer_id)

        while True:
            if self.num_map_processes == 0: 
                print("⏩️✅ Reducer #{} finished. Output:{}".format(self.reducer_id, self.word_storage))
                self.write_output()
                break

            mapper_output = self.input_queue.get()
            
            # Expect a key-value tuple
            if len(mapper_output) != 2:
                # Something has gone wrong. Skip this line.
                print("Reducer: something went wrong")
                continue

            key, value = mapper_output[0], mapper_output[1]

            # Keep waiting for mapper output
            if key is None and value is None: 
                print("⏩️ Reducer #{} waiting for mapper".format(self.reducer_id))
                continue
            
            # Keep track if the Mappers terminated
            if key == "EOF" and value is None: 
                self.num_map_processes = self.num_map_processes - 1
            elif key in self.word_storage:
                self.word_storage[key] += value
            else:
                self.word_storage[key] = value
        return

    def write_output(self):
        """ Write results to a file """
        output_filename = f"output/reducer_output_{self.reducer_id}.txt"
        with open(output_filename, 'w') as output_file:
            for key, value in self.word_storage.items():
                output_file.write(f"{key}\t{value}\n")