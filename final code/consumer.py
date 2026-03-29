

#cpu bounds
import csv
import pandas as pd

import rarfile
from ahocorasick import Automaton
import re
from threading import Thread
import time
import openpyxl
from functools import reduce
import os



#cpu bounds
class ConsumerThread(Thread):
    def __init__(self, input_queue,input_queueAho, success_queue,success_queueAho, fail_queue,fail_queueAho, badwordsqueue, bad_words_queueAho, timedict, head):
        super().__init__()
        self.input_queue = input_queue
        self.success_queue = success_queue
        self.fail_queue =fail_queue
        self.badWordsQueue =badwordsqueue
        self.badWords=self.badWordsQueue.get()
        self.filterCount = 0
        self.writeCount = 0
        
        
        self.input_queueAho = input_queueAho
        self.success_queueAho = success_queueAho
        self.fail_queueAho =fail_queueAho
        self.badWordsQueueAho =bad_words_queueAho
        self.badWordsAho = self.badWordsQueueAho.get()
        self.automaton = Automaton()
        for word in self.badWordsAho:
            self.automaton.add_word(word, (1, word))
        self.automaton.make_automaton()
        self.writeCountAho = 0
        self.filterCountAho = 0
        
        self.timedict=timedict
        self.head=head
        
        
    def Reg_Filter(self):
        while True:
            if not self.input_queue.empty():
              
                chunk = self.input_queue.get()

                if chunk is None:
                    self.success_queue.put(None)
                    self.fail_queue.put(None)
                    break
            
                #filtering
                start_time=time.time()
            
                
                boolList = [ ~chunk.iloc[:,head].str.contains(self.badWords, regex = True, flags = re.I,na=False) for head in self.head ]
                bool_checker = self.check_bool(boolList)

                healthy_df = chunk[bool_checker]
                # print(healthy_df)
                unhealthy_df = chunk[~bool_checker]
                # print(unhealthy_df)
                end_time=time.time()
        

                self.success_queue.put(healthy_df)
                self.fail_queue.put(unhealthy_df)
                
                self.timedict['filtering'].append(end_time-start_time)
                boolList.clear()
                
                self.write_csv2(self.timedict['filtering'][self.filterCount],self.timedict['chunksize'],"Filtering_Times_Regex")
                self.filterCount += 1
                if self.filterCount % 20 == 0 :
                    print("finished filtering Regex number: "+ str(self.filterCount))
                
                start_time=time.time()
                self.write_csv(self.success_queue,"Healty Record Regular")
                self.write_csv(self.fail_queue,"UnHealty Record Regular")
                end_time=time.time()
                self.timedict['writing'].append(end_time-start_time)
                
                self.write_csv2(self.timedict['writing'][self.writeCount],self.timedict['chunksize'],"Writing_Times_Regex")
                self.writeCount += 1
                if self.writeCount % 20 == 0 :
                    print("finished writing Regex number: "+ str(self.writeCount))




    def Aho_Filter(self):
        # self.create_Trie_UsingPyAcho()
        # self.automaton.make_automaton()

        while True:
            if not self.input_queueAho.empty():
                item=self.input_queueAho.get()

                if item is None:
                    self.success_queueAho.put(None)
                    self.fail_queueAho.put(None)
                    break

                start = time.time()
                #Filter by multiple columns and save it in list
                #Use "~" to change True to False and False to True 
                #we change True to False because we want to filter bad words

                
                
             
                # boolList = [ ~item.iloc[:,head].apply(lambda x : len(list(self.automaton.iter(x.lower()))) != 0) for head in self.head]
                boolList = [ ~item.iloc[:,head].apply(lambda x : len(list(self.automaton.iter(x.lower()))) != 0 if isinstance(x, str) else False) for head in self.head]

                bool_checker = self.check_bool(boolList)
                end = time.time()
                self.timedict["filteringAho"].append(end-start)
                
                self.write_csv2(self.timedict['filteringAho'][self.filterCountAho],self.timedict['chunksizeAho'],"Filtering_Times_Aho")
                self.filterCountAho += 1
                if self.filterCountAho % 20 == 0 :
                    print("finished Filtering Aho number: "+ str(self.filterCountAho))
                
                healthy_df = item[bool_checker]
                unhealthy_df = item[~bool_checker]
                self.success_queueAho.put(healthy_df)
                self.fail_queueAho.put(unhealthy_df)
                boolList.clear()
                
                
                start_time=time.time()
                self.write_csv(self.success_queueAho,"Healty Record Aho")
                self.write_csv(self.fail_queueAho,"UnHealty Record Aho")
                end_time=time.time()
                self.timedict['writingAho'].append(end_time-start_time)
                
                
                self.write_csv2(self.timedict['writingAho'][self.writeCountAho],self.timedict['chunksizeAho'],"Writing_Times_Aho")
                self.writeCountAho += 1
                if self.writeCountAho % 20 == 0 :
                    print("finished writing Aho number: "+ str(self.writeCountAho))
                    
                         
        
    def run(self):
        
        # self.Reg_Filter()
        # self.Aho_Filter()
        
        reg_thread = Thread(target=self.Reg_Filter)
        aho_thread = Thread(target=self.Aho_Filter)

        reg_thread.start()
        aho_thread.start()

        reg_thread.join()
        aho_thread.join()
        
        data = {k: pd.Series(v) for k, v in self.timedict.items()}
        df = pd.DataFrame(data)
        df.to_csv('outputooo2.csv', index=False)
        # df = pd.DataFrame.from_dict(self.timedict, orient='index').transpose()
        # df.to_csv('outputoooo.csv', index=False)
        
        self.ExceLwriter()   



    #pythoinc for loop instead of 
    #x = BoolList[0]
    #   for i in BoolList:
    #      x = x & i
    def check_bool(self, BoolList):
       return reduce(lambda x, y: x & y, BoolList)
    





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

            record = queue.get()  
            # Check if the file already exists
            if not os.path.exists(fileName):
                # write the first chunk with header
                record.to_csv(fileName, mode='w', header=True, index=False) 

            else:    
                # append chunk without header
                record.to_csv(fileName, mode='a', header=False, index=False) 
                        

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


        
    def ExceLwriter(self):

        #measure reading total time and avg time for regular expression
        total_time_of_read_Reg=sum(self.timedict['reading'])
        avg_time_of_read_Reg=total_time_of_read_Reg/self.timedict['number of chunks']

        #measure filtering total time and avg time for regular expression
        total_time_of_filter_Reg=sum(self.timedict['filtering'])
        avg_time_of_filter_Reg=total_time_of_filter_Reg/self.timedict['number of chunks']
        #measure writing total time and avg time for regular expression
        total_time_of_writing_Reg=sum(self.timedict['writing'])
        avg_time_of_writing_Reg=total_time_of_writing_Reg/self.timedict['number of chunks']

        #measure processing total time and avg for regular expression
        total_time_of_processing_Reg=sum(self.timedict['reading'])+sum(self.timedict['filtering'])+sum(self.timedict['writing'])
        avg_time_of_processing_Reg=total_time_of_processing_Reg/self.timedict['number of chunks']
        
        
        
        
        #measure reading total time and avg time for Aho corasick
        total_time_of_read_Aho=sum(self.timedict['readingAho'])
        avg_time_of_read_Aho=total_time_of_read_Aho/self.timedict['number of chunksAho']

        #measure filtering total time and avg time for Aho corasick
        total_time_of_filter_Aho=sum(self.timedict['filteringAho'])
        avg_time_of_filter_Aho=total_time_of_filter_Aho/self.timedict['number of chunksAho']
        #measure writing total time and avg time for Aho corasick
        total_time_of_writing_Aho=sum(self.timedict['writingAho'])
        avg_time_of_writing_Reg=total_time_of_writing_Aho/self.timedict['number of chunksAho']

        #measure processing total time and avg for regular Aho corasick
        total_time_of_processing_Aho=sum(self.timedict['readingAho'])+sum(self.timedict['filteringAho'])+sum(self.timedict['writingAho'])
        avg_time_of_processing_Aho=total_time_of_processing_Aho/self.timedict['number of chunksAho']
        
        total_time_of_processing_All = total_time_of_processing_Aho + total_time_of_processing_Reg
        
        
        data = {
            "D.frame size":self.timedict['chunksize'] ,
            
            'avg_Reading_Reg': avg_time_of_read_Reg,
            'avg_filtering_Reg': avg_time_of_filter_Reg,
            'Total_processing_Time_Reg': total_time_of_processing_Reg,
            'avg_processing_Time_Reg': avg_time_of_processing_Reg,
            
            'avg_Reading_Aho': avg_time_of_read_Aho,
            'avg_filtering_Aho': avg_time_of_filter_Aho,
            'Total_processing_Time_Aho': total_time_of_processing_Aho,
            'avg_processing_Time_Aho': avg_time_of_processing_Aho,
            'Total_processing_Time_ALL': total_time_of_processing_All


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


