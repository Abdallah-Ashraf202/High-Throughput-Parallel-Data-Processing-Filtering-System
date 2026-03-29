from producer import ProducerThread
from consumer import ConsumerThread
# from consumer_Aho import ConsumerThread
# from producer_Aho import ProducerThread
from queue import Queue

def main(data_file_path, bad_words_file_path, specify_cols, head, rows_at_chunk):
    """
    This function sets up the producer-consumer model and starts the threads for processing the data.
    """
    # Set up the queues for the producer-consumer model
    input_queue = Queue()
    success_queue = Queue()
    fail_queue = Queue()
    bad_words_queue = Queue()
    input_queueAho = Queue()
    success_queueAho = Queue()
    fail_queueAho = Queue()
    bad_words_queueAho = Queue()

    # Initialize the time dictionary to track the time taken by each step of the process
    timedict = {
        'chunksize': int(rows_at_chunk),
        'number of chunks': None,
        'reading': [],
        'filtering': [],
        'writing': [],
        
        'chunksizeAho': int(rows_at_chunk),
        'number of chunksAho': None,
        'readingAho': [],
        'filteringAho': [],
        'writingAho': []
    }

    # Create producer and consumer threads
    producer = ProducerThread(data_file_path, rows_at_chunk, input_queue,input_queueAho, timedict, bad_words_file_path, bad_words_queue ,bad_words_queueAho ,specify_cols)
    consumer = ConsumerThread(input_queue,input_queueAho, success_queue,success_queueAho, fail_queue,fail_queueAho, bad_words_queue, bad_words_queueAho, timedict, head)
    # producer_Aho = ProducerThread(data_file_path, rows_at_chunk, input_queue, timedict, bad_words_file_path, bad_words_queue, specify_cols)
    # consumer_Aho = ConsumerThread(input_queue, success_queue, fail_queue, bad_words_queue, timedict, head)
   

    # Start the threads
    producer.start()
    # producer_Aho.start()
    
    consumer.start()
    # consumer_Aho.start()


    # Wait for the threads to complete
    producer.join()
    # producer_Aho.join()
    
    consumer.join()
    # consumer_Aho.join()

    # Print the results
    print("Processing completed with rows_at_chunk = {}".format(rows_at_chunk))
    print("Time taken for each step:")
    print("Reading: ", timedict['reading'])
    print("Filtering: ", timedict['filtering'])
    print("Writing: ", timedict['writing'])

if __name__ == "__main__":
    # Set the file paths and other parameters
    specify_cols = [0, 2, 6]
    data_file_path = r'D:\University\CS342\40k.rar'
    bad_words_file_path = r'D:\University\Project Hussien\badWordss.csv'
    head = [0, 1, 2]
    
    rows_at_chunk = 10000

    # Process the data with different values of rows_at_chunk
    while rows_at_chunk <= 10000:
        main(data_file_path, bad_words_file_path, specify_cols, head, rows_at_chunk)
        rows_at_chunk += 10000

###### DONE ######
