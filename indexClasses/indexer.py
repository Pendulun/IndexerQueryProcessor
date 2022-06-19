from multiprocessing import Process, Queue, Semaphore, Lock
from parserClasses.myparser import TextParser
from indexClasses.index import Index
from warcio import ArchiveIterator
import resource
import pathlib
import psutil
import pickle
import sys
import os


class Indexer():

    STOP_FLAG = -1
    MEGABYTE = 1024 * 1024

    def __init__(self, corpus_dir_path:str, index_dir_path:str):
        self._corpus_dir_path = pathlib.Path(corpus_dir_path)
        assert self._corpus_dir_path.exists()

        self._index_dir_path = pathlib.Path(index_dir_path)
        self._index_dir_path.mkdir(parents=True, exist_ok=True)
        assert self._index_dir_path.exists()

        self._id_to_doc_file = "" #Path

        #Dining Savages Problem
        self._empty_text_queue_sem = Semaphore()
        self._full_text_queue_sem = Semaphore()
        self._text_queue = Queue()
        self._get_from_text_queue_lock = Lock()
        
        self._num_workers = 0
        self._n_queue_factor = 4
    
    @property
    def id_to_doc_file(self):
        return self._id_to_doc_file
    
    @id_to_doc_file.setter
    def id_to_doc_file(self, new_id_to_doc_file):
        self._id_to_doc_file = new_id_to_doc_file
    
    @property
    def id_to_doc_file(self):
        return self._id_to_doc_file
    
    @id_to_doc_file.setter
    def id_to_doc_file(self, new_id_to_doc_file):
        self._id_to_doc_file = new_id_to_doc_file
    
    def set_n_queue_factor(self, new_factor:int):
        if type(new_factor) != int:
            raise TypeError('new_factor should be an int!')

        self._n_queue_factor = new_factor
    
    def _memory_limit(self, mega_bytes:int):
        limit = mega_bytes * Indexer.MEGABYTE
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

    def index_multiprocess(self, max_mem_mb_used:int, num_workers:int = 1):
        """
        There is always 2 proccess at minimum. num_workers sets only the number of processes that tokenize and index
        the texts. There is (only) another one for reading the files. 
        """
        self._num_workers = num_workers
        READING_PROC_MAX_MEM_MB = 30
        READING_PROC_IDX = 0

        reading_proccess = Process(target=self._get_from_corpus_files, args=(READING_PROC_IDX, READING_PROC_MAX_MEM_MB))
        tokenizing_proccesses = []

        TOTAL_MEM_FOR_TOKENIZING_INDEXING_PROCESSES = max_mem_mb_used - READING_PROC_MAX_MEM_MB
        MEM_PER_INDEXING_PROC = TOTAL_MEM_FOR_TOKENIZING_INDEXING_PROCESSES//num_workers

        for worker_id in range(READING_PROC_IDX+1, READING_PROC_IDX+num_workers+1):
            tokenizing_proccesses.append(Process(target=self._tokeninze_and_index_text, args=(worker_id, MEM_PER_INDEXING_PROC)))

        reading_proccess.start()
        [proc.start() for proc in tokenizing_proccesses]
        
        reading_proccess.join()
        [proc.join() for proc in tokenizing_proccesses]
    
    def _get_from_corpus_files(self, my_id:int, max_mem_mb:int):
        #self._memory_limit(max_mem_mb)
        max_mem_usage = max_mem_mb * Indexer.MEGABYTE
        resource.setrlimit(resource.RLIMIT_AS, (max_mem_usage, max_mem_usage))

        DOCS_TO_ADD_AS_NEEDED = self._num_workers*self._n_queue_factor
        curr_doc_id = 0
        docs_added_to_queue = 0

        id_to_doc_set = set()
        if self._id_to_doc_file.exists():
            self._id_to_doc_file.unlink()

        try:
            warc_file_count = 0
            MAX_WARC_FILES = 1
            for curr_warc_file_name in self._corpus_dir_path.glob('*.warc.gz.kaggle'):
                warc_file_count += 1
                with open(curr_warc_file_name, 'rb') as curr_warc_file:
                    for record in ArchiveIterator(curr_warc_file):
                        
                        #Dining Savages Problem
                        if docs_added_to_queue == 0:
                            self._empty_text_queue_sem.acquire()

                        text = record.raw_stream.read().decode().strip()
                        doc_url = record.rec_headers.get_header('WARC-Target-URI')
                        
                        curr_doc_id += 1
                        docs_added_to_queue += 1

                        id_to_doc_set.add((curr_doc_id, doc_url))

                        curr_mem_usage = psutil.Process(os.getpid()).memory_info().rss
                        if  curr_mem_usage >= max_mem_usage:
                            self.write_id_to_doc_map_to(id_to_doc_set, self._id_to_doc_file)
                            id_to_doc_set.clear()
                         
                        self._text_queue.put((curr_doc_id, text))

                        if docs_added_to_queue == DOCS_TO_ADD_AS_NEEDED:
                            docs_added_to_queue = 0
                            self._full_text_queue_sem.release()
                
                #ONLY ONE WARC FILE
                if warc_file_count == MAX_WARC_FILES:
                    break
            
            if docs_added_to_queue != 0:
                self._full_text_queue_sem.release()
                docs_added_to_queue = 0
            
            #As we use a Queue, we must wait for the queue to be empty to add
            # self._num_workers STOP_FLAGS 
            self._put_stop_flag_for_workers()

            self.write_id_to_doc_map_to(id_to_doc_set, self._id_to_doc_file)
            id_to_doc_set.clear()
            
        except MemoryError:
            sys.stderr.write(f'\n\nERROR: Memory Exception for READER. total_docs_added: {curr_doc_id}\n')

    def _put_stop_flag_for_workers(self):
        self._empty_text_queue_sem.acquire()
        for _ in range(self._num_workers):
            self._text_queue.put((Indexer.STOP_FLAG, Indexer.STOP_FLAG))
        self._full_text_queue_sem.release()
    
    def write_id_to_doc_map_to(self, my_map, file:pathlib.Path):
        with open(file, 'ab') as id_doc_map_file:
            pickle.dump(my_map, id_doc_map_file)
    
    def load_id_to_doc_map_from(cls, file:str) -> set:
        my_map = set()
        with open(file, 'rb') as id_doc_map_file:
            while True:
                try:
                    my_map.update(pickle.load(id_doc_map_file))
                except EOFError:
                    break
        
        return my_map
    
    def _tokeninze_and_index_text(self, my_id:int, max_mem_mb:float):
        # self._memory_limit(max_mem_mb)
        max_mem_usage = max_mem_mb * Indexer.MEGABYTE
        resource.setrlimit(resource.RLIMIT_AS, (max_mem_usage, max_mem_usage))

        my_index = Index()
        curr_mem_usage = 0

        num_proc_file = 0
        sub_index_file = self._index_dir_path/f'index-{my_id}-{num_proc_file}.pickle'
        if sub_index_file.exists():
            sub_index_file.unlink()

        try:
            while True:    
                
                #Dining Savages Problem
                self._get_from_text_queue_lock.acquire()

                if self._text_queue.empty():
                    self._empty_text_queue_sem.release()
                    self._full_text_queue_sem.acquire()

                info_from_reading_proc = self._text_queue.get()
                
                doc_id = info_from_reading_proc[0]
                text_from_corpus = info_from_reading_proc[1]
                self._get_from_text_queue_lock.release()

                curr_mem_usage = psutil.Process(os.getpid()).memory_info().rss
                if curr_mem_usage > max_mem_usage:
                    my_index.save_to_pickle_and_clear(sub_index_file)
                    num_proc_file += 1
                    sub_index_file = self._index_dir_path/f'index-{my_id}-{num_proc_file}.pickle'
                    if sub_index_file.exists():
                        sub_index_file.unlink()

                received_should_stop_signal = info_from_reading_proc[0] == Indexer.STOP_FLAG
                if not received_should_stop_signal:

                    text_distribuition = TextParser.get_distribuition_of(text_from_corpus)
                    my_index.add_from_distribuition(text_distribuition, doc_id)
                    
                else:
                    break
            
            my_index.save_to_pickle_and_clear(sub_index_file)

        except MemoryError:
            sys.stderr.write(f'\n\nERROR: Memory Exception in Process {my_id}. MEM_USED: {curr_mem_usage}/{max_mem_usage}. Index size: {my_index.size}\n')
    
    def reset(self):
        self._empty_text_queue_sem = Semaphore()
        self._full_text_queue_sem = Semaphore()
        self._text_queue = Queue()
        self._get_from_text_queue_lock = Lock()