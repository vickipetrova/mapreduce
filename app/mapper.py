import os
import hashlib

class Mapper:
    """ 
    A mapper class to perform the map stage of MapReduce 
    Attributes
    ----------
    output_queues: list[Queue]
        The list of reducer queues
    num_reducer_processes: int
        The number of reducing processes (the size of the output_queues list)
    data_directory: string
        The mapper process is given a directory to use as a source of data

    Methods
    -------
    map:
        Reads each file from a file directory. Each file is read line by line (not all at once) and 
        a stream of mapped key-count pairs are sent to the target reducer
    hash_word:

    """

    def __init__ (self, output_queues, data_directory):
        self.output_queues = output_queues
        self.num_reducer_processes = len(output_queues)
        self.data_directory = data_directory

    def map(self):
        print("🗺️ MAP operation initialized")

        # Read files from the specified data directory
        for filename in os.listdir(self.data_directory):
            
            filepath = os.path.join(self.data_directory, filename)

            with open(filepath, 'r') as file:
                # Read the input line by line
                for line in file:
                    # Clean data and split line into words
                    line = line.strip()
                    words = line.split()

                    for word in words:
                        # Hash the word to determine the reducer
                        reducer_id = self.hash_word(word)
                        
                        # Send the output to the corresponding reducer queue
                        self.output_queues[reducer_id].put((word, 1))
            
        # Send EOF message to each reducer's queue
        for queue in self.output_queues:
            queue.put(("EOF", None))
        
        print("🗺️❌ Mapper is done: EOF message")

    def hash_word(self, word):
        """
        Non-cryptographic hash function with modulo
        
        The hashlib library ensures a consistend and predictable hashing.
        1) First, the word is hashed using M5 hashing algorithm and turned to bytes. 
        2) Then, the hashed word is turned into an integer
        3) The modulo operator is used to determine the target reducers
        """
        hash_object = hashlib.md5(word.encode())
        hash_value = int(hash_object.hexdigest(), 16) % self.num_reducer_processes
        return hash_value