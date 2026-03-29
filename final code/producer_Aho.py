import pickle
import pandas as pd
from threading import Thread
import time
import re
import rarfile
from ahocorasick import Automaton

rarfile.UNRAR_TOOL = r'D:\Programs\Unrar\UnRAR.exe'

# i/o bounds
class ProducerThread(Thread):
    def __init__(
        self,
        filename,
        chunksize,
        input_queue,
        timedict,
        badwords_filename,
        badWordsQueue,
        specify_cols,
    ):
        super().__init__()
        self.filename = rarfile.RarFile(filename)
        self.chunksize = chunksize
        self.input_queue = input_queue
        self.timedict = timedict
        self.patterns = pd.read_csv(badwords_filename, header=None).iloc[:, 0].tolist()
        self.automaton = Automaton()
        for index, word in enumerate(self.patterns):
            self.automaton.add_word(word, (index, word))
        self.automaton.make_automaton()
        self.badWordsQueue = badWordsQueue
        self.badWordsQueue.put(self.patterns)
        self.specify_cols = specify_cols
        self.num_chunks = 0
        # assign the chunksize value to timedict
        self.timedict["chunksize"] = self.chunksize
        self.readCount = 0

    # using yield
    def read_csv_chunks(self, filename, chunksize):
        chunks = pd.read_csv(
            filename.open(filename.namelist()[0]),
            usecols=self.specify_cols,
            
            iterator=True,
            nrows=10000,
        )
        for chunk in chunks:
            yield chunk

    def run(self):

        # Create generator for chunks of data
        chunks = self.read_csv_chunks(self.filename, self.chunksize)

        start_time = time.time()
        # Process chunks of data until there are no more chunks
        for chunk in chunks:
            end_time = time.time()
            self.input_queue.put(chunk)
            self.timedict["reading"].append(end_time - start_time)
            self.readCount += 1
            if self.readCount % 1 == 0:
                print("Finished reading number:" + str(self.readCount))

            self.num_chunks += 1
            start_time = time.time()

        # send number of chunks in dict that has time statistics as well
        self.timedict["number of chunks"] = self.num_chunks

        # Signal end of input and compute read time statistics
        self.input_queue.put(None)

        # create automaton pickle file
        with open('automaton.pickle', 'wb') as f:
            pickle.dump(self.automaton, f)
