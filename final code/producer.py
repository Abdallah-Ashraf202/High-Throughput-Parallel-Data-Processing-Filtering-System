import csv
import os
import pandas as pd
from threading import Thread
import time
import re
import rarfile
import pickle
from ahocorasick import Automaton
rarfile.UNRAR_TOOL = r'D:\Programs\Unrar\UnRAR.exe'
import consumer
#i/o bounds
class ProducerThread(Thread):
    def __init__(self, filename, chunksize, input_queue,input_queueAho,timedict,badwords_filename ,badWordsQueue,bad_words_queueAho,specify_cols):
        super().__init__()
        self.filename = rarfile.RarFile(filename)
        self.chunksize = chunksize
        self.timedict=timedict
        self.specify_cols=specify_cols
        self.num_chunks =0
        
        self.input_queue = input_queue
        self.badWordsQueue=badWordsQueue
        self.patterns = pd.read_csv(badwords_filename).values.tolist()
        self.badWords =('|'.join(re.escape(x[0]) for x in self.patterns)) 
        self.badWordsQueue.put(self.badWords)
        
        
        self.input_queueAho = input_queueAho
        self.badWordsQueueAho=bad_words_queueAho
        self.patternsAho = pd.read_csv(badwords_filename, header=None).iloc[:, 0].tolist()
        self.automaton = Automaton()
        for index, word in enumerate(self.patternsAho):
            self.automaton.add_word(word, (index, word))
        self.automaton.make_automaton()
        self.badWordsQueueAho = bad_words_queueAho
        self.badWordsQueueAho.put(self.patternsAho)
        
        
        
        #assign the chunksize value to timedict 
        self.timedict['chunksize']=self.chunksize
        self.readCount = 0
    
        
        #using yield

    def read_csv_chunks(self,filename, chunksize):
        chunks = pd.read_csv(filename.open(filename.namelist()[0]), usecols=self.specify_cols , chunksize=chunksize, iterator=True)
        for chunk in chunks:
            yield chunk

    def run(self):
        
        # Create generator for chunks of data
        chunks = self.read_csv_chunks(self.filename,self.chunksize)
        
        start_time = time.time()
        # Process chunks of data until there are no more chunks
        for chunk in chunks:
            end_time=time.time()
           
            self.input_queue.put(chunk)
            self.input_queueAho.put(chunk)
            self.timedict['reading'].append(end_time-start_time)
            self.timedict['readingAho'].append(end_time-start_time)
            
            self.write_csv2(self.timedict['reading'][self.readCount],self.timedict['chunksize'],"Reading_Times_Regex")
            self.readCount += 1
            if self.readCount % 20 == 0 :
                print("Finished reading for Aho and regex number:"+ str(self.readCount) )
            
            self.num_chunks += 1
            start_time = time.time()
    
        #send number of chunks in dict that has time statistics as well
        self.timedict['number of chunks']=self.num_chunks
        self.timedict['number of chunksAho']=self.num_chunks
        
        # Signal end of input and compute read time statistics
        self.input_queue.put(None)
        self.input_queueAho.put(None)
        # create automaton pickle file
        with open('automaton.pickle', 'wb') as f:
            pickle.dump(self.automaton, f)
            
            
            
            
            
            
    # def write_csv2(self, value, fileName, columnName):
    #     if not os.path.exists("output"):
    #         os.makedirs("output")

    #     # Create folder to store healthy and unhealthy files
    #     if not os.path.exists(f"output/FrameSize{str(self.timedict['chunksize'])}"):
    #         os.makedirs(f"output/FrameSize{str(self.timedict['chunksize'])}")

    #     fileName = f"output/FrameSize{str(self.timedict['chunksize'])}/{fileName} Chunk.csv"  # add the .csv extension

    #     # Check if the file already exists
    #     if not os.path.exists(fileName):
    #         # Create new CSV file with header
    #         with open(fileName, 'w', newline='') as csvfile:
    #             writer = csv.writer(csvfile)
    #             writer.writerow([columnName])
    #             writer.writerow([value])
    #     else:
    #         # Append data to existing CSV file
    #         with open(fileName, 'a', newline='') as csvfile:
    #             writer = csv.writer(csvfile)
    #             writer.writerow([value])    


    def write_csv2(self, value1, value2, fileName):
        if not os.path.exists("output"):
            os.makedirs("output")

        # Create folder to store healthy and unhealthy files
        if not os.path.exists(f"output/FrameSize{str(self.timedict['chunksize'])}"):
            os.makedirs(f"output/FrameSize{str(self.timedict['chunksize'])}")

        fileName = f"output/FrameSize{str(self.timedict['chunksize'])}/{fileName} Chunk.csv"  # add the .csv extension

        # Check if the file already exists
        if not os.path.exists(fileName):
            # Create new CSV file with header
            with open(fileName, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Column1", "Column2"])
                writer.writerow([value1, value2])
        else:
            # Append data to existing CSV file
            with open(fileName, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([value1, value2])
    

    # def write_csv2(self, value, fileName, columnName):
    #     if not os.path.exists("output"):
    #         os.makedirs("output")

    #     # Create folder to store healthy and unhealthy files
    #     if not os.path.exists(f"output/FrameSize{str(self.timedict['chunksize'])}"):
    #         os.makedirs(f"output/FrameSize{str(self.timedict['chunksize'])}")

    #     fileName = f"output/FrameSize{str(self.timedict['chunksize'])}/{fileName} Chunk.csv"  # add the .csv extension

    #     # Check if the file already exists
    #     if not os.path.exists(fileName):
    #         # Create new CSV file with header
    #         with open(fileName, 'w', newline='') as csvfile:
    #             writer = csv.DictWriter(csvfile, fieldnames=[columnName])
    #             writer.writeheader()
    #             writer.writerow({columnName: value})
    #     else:
    #         # Append data to existing CSV file
    #         with open(fileName, 'a', newline='') as csvfile:
    #             fieldnames = []
    #             with open(fileName, 'r', newline='') as f:
    #                 reader = csv.DictReader(f)
    #                 fieldnames = reader.fieldnames

    #             if columnName not in fieldnames:
    #                 fieldnames.append(columnName)

    #             writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #             if csvfile.tell() == 0:
    #                 writer.writeheader()

    #             row = {field: '' for field in fieldnames}
    #             row[columnName] = value
    #             writer.writerow(row)















