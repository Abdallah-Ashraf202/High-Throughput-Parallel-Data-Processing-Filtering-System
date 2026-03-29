import os
import openpyxl
#cpu bounds
import pandas as pd
from threading import Thread
import time
import re
import rarfile
from ahocorasick import Automaton
from functools import reduce

rarfile.UNRAR_TOOL = r'D:\Programs\Unrar\UnRAR.exe'

# cpu bounds
class ConsumerThread(Thread):
    def __init__(self,input_queue,success_queue,fail_queue,badWordsQueue,timedict,head,specify_cols,):
        super().__init__()
        self.input_queue = input_queue
        self.success_queue = success_queue
        self.fail_queue = fail_queue
        self.badWordsQueue = badWordsQueue
        self.timedict = timedict
        self.head = head
        self.badWords = self.badWordsQueue.get()
        self.filterCount = 0
        self.writeCount = 0
        self.automaton = Automaton()
        for word in self.badWords:
            self.automaton.add_word(word, (1, word))
        self.automaton.make_automaton()

    # def check_bad_words(self, text):
    #     """
    #     Checks if any bad words exist in the input text using Aho-Corasick algorithm.
    #     """
    #     matches = []
    #     for end_index, (insert_order, original_value) in self.automaton.iter(text):
    #         matches.append((original_value, end_index - len(original_value) + 1))
    #     return matches

    # def filter_bad_words(self, data):
    #     """
    #     Filters out rows containing bad words.
    #     """
    #     for index, row in data.iterrows():
    #         text = reduce(lambda x, y: str(x) + ' ' + str(y), row.tolist())
    #         matches = self.check_bad_words(text)
    #         if matches:
    #             self.fail_queue.put((matches, row.tolist()))
    #         else:
    #             self.success_queue.put(row.tolist())
                
        
        
        
    # def create_Trie_UsingPyAcho(self):
    #     for word in  self.badWords.values.tolist():
    #         self.automaton.add_word(word[0].lower(), word[0].lower())
    #     return self.automaton    
    
    def check_bool(self, BoolList):
        x = BoolList[0]
        for i in BoolList:
            x = x & i
        return x
   
    def Filter(self):
        
        
        # self.create_Trie_UsingPyAcho()
        # self.automaton.make_automaton()

        while True:
            if not self.input_queue.empty():
                item=self.input_queue.get()

                if item is None:
                    self.success_queue(None)
                    self.fail_queue(None)
                    break

                start = time.time()
                #Filter by multiple columns and save it in list
                #Use "~" to change True to False and False to True 
                #we change True to False because we want to filter bad words

                
                
             
                # boolList = [ ~item.iloc[:,head].apply(lambda x : len(list(self.automaton.iter(x.lower()))) != 0) for head in self.head]
                boolList = [ ~item.iloc[:,head].apply(lambda x : len(list(self.automaton.iter(x.lower()))) != 0 if isinstance(x, str) else False) for head in self.head]

                bool_checker = self.check_bool(boolList)
                end = time.time()
                
                self.filterCount += 1
                self.timedict["filtering"].append(end-start)
                if self.filterCount % 1 == 0 :
                    print("finished Filtering number: "+ str(self.filterCount))
                
                healthy_df = item[bool_checker]
                unhealthy_df = item[~bool_checker]
                self.success_queue.put(healthy_df)
                self.fail_queue.put(unhealthy_df)
                boolList.clear()
                
                
                start_time=time.time()
                self.write_csv(self.success_queue,"Healty Record")
                self.write_csv(self.fail_queue,"UnHealty Record")
                end_time=time.time()
                self.timedict['writing'].append(end_time-start_time)
                
                self.writeCount += 1
                if self.writeCount % 1 == 0 :
                    print("finished writing number: "+ str(self.writeCount)) 
        self.ExceLwriter()
                
                
    def run(self):
        self.Filter()                  
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
                
    # def run(self):
    #     while True:
    #         chunk = self.input_queue.get()

    #        # End of input
    #         if chunk is None:
    #             # Signal end of success_queue and fail_queue
    #             self.success_queue.put(None)
    #             self.fail_queue.put(None)

    #             # Print statistics
    #             print("Filtering completed with rows_at_chunk = {}".format(self.timedict["chunksize"]))
    #             print("Time taken for each step:")
    #             print("Reading: ", self.timedict["reading"])
    #             print("Filtering: ", self.timedict["filtering"])
    #             print("Writing: ", self.timedict["writing"])
    #             break

    #         start_time = time.time()
    #         self.filter_bad_words(chunk)
    #         end_time = time.time()
    #         self.timedict["filtering"].append(end_time - start_time)
    #         self.filterCount += 1
    #         if self.filterCount % 1 == 0:
    #             print("Finished filtering number:" + str(self.filterCount))

    #         start_time=time.time()
            
    #         # success_list = list(self.success_queue.queue)
    #         # self.write_csv(success_list, "Healty Record")
    #         # fail_list = list(self.fail_queue.queue)
    #         # self.write_csv(fail_list, "UnHealty Record")
             
    #         self.write_csv(self.success_queue,"Healty Record")
    #         self.write_csv(self.fail_queue,"UnHealty Record")
                
    #         # self.write_csv(list(self.success_queue),"Healty Record")
    #         # self.write_csv(list(self.fail_queue),"UnHealty Record")
                        
    #         end_time = time.time()


    #         self.timedict['writing'].append(end_time-start_time)
    #         self.writeCount += 1
    #         if self.writeCount % 1 == 0 :
    #             print("finished writing number: "+ str(self.writeCount))

    #     self.ExceLwriter()
                




    def write_csv(self, queue, fileName):
        if not os.path.exists("output"):
            os.makedirs("output")
        
        
        #create folder store healty and unhealty files
        if not os.path.exists("output/FrameSize"+str(self.timedict['chunksize'])):
            os.makedirs("output/FrameSize"+str(self.timedict['chunksize']))

        fileName="output/FrameSize"+str(self.timedict['chunksize'])+"/"+fileName +".csv" # add the .csv extension
        
        while True:
            if queue.empty():
                break
            # print(queue.queue)
            record = queue.get()
            # record = pd.DataFrame(record)  
            # Check if the file already exists
            if not os.path.exists(fileName):
                # write the first chunk with header
                record.to_csv(fileName, mode='w', header=True, index=False)
                 

            else:    
                # append chunk without header
                record.to_csv(fileName, mode='a', header=False, index=False) 
                        

   

        
    def ExceLwriter(self):

        #measure reading total time and avg time
        total_time_of_read=sum(self.timedict['reading'])
        avg_time_of_read=total_time_of_read/self.timedict['number of chunks']

        #measure filtering total time and avg time
        total_time_of_filter=sum(self.timedict['filtering'])
        avg_time_of_filter=total_time_of_filter/self.timedict['number of chunks']
        #measure writing total time and avg time
        total_time_of_writing=sum(self.timedict['writing'])
        avg_time_of_writing=total_time_of_writing/self.timedict['number of chunks']

        #measure processing total time and avg time
        total_time_of_processing=sum(self.timedict['reading'])+sum(self.timedict['filtering'])+sum(self.timedict['writing'])
        avg_time_of_processing=total_time_of_processing/self.timedict['number of chunks']
        
        
        data = {
            "D.frame size":self.timedict['chunksize'] ,
            'avg_Reading Time': avg_time_of_read,
            'avg_filtering Time': avg_time_of_filter,
            'Total_processing_Time': total_time_of_processing,
            'avg_processing_Time': avg_time_of_processing

            }
        
        headers = list(data.keys())
           # check if the file exists
        try:
            workbook = openpyxl.load_workbook("./output/output.xlsx")
            worksheet = workbook.active
        except FileNotFoundError:
        #create a new workbook and worksheet if the file doesn't exist
            workbook = openpyxl.Workbook()
            worksheet = workbook.active
            worksheet.append(headers)
        
        # append data to the worksheet
        values = [data[header] for header in headers]
        worksheet.append(values)
            # save the workbook
        workbook.save('./output/output.xlsx')


