from multiprocessing import Process, Queue, Semaphore, Lock
from parserClasses.myparser import TextParser
from indexClasses.index import Index
from warcio import ArchiveIterator
import pathlib

class Indexer():

    MAX_FILES_INDEXED = 10
    MIN_FILE_CHAR_COUNT = 10
    STOP_FLAG = -1

    def __init__(self, corpus_dir_path:str, index_dir_path:str):
        self._corpus_dir_path = pathlib.Path(corpus_dir_path)
        assert self._corpus_dir_path.exists()

        self._index_dir_path = pathlib.Path(index_dir_path)
        self._index_dir_path.mkdir(parents=True, exist_ok=True)

        assert self._index_dir_path.exists()

        self._index = Index()

        self._empty_text_queue_sem = Semaphore()
        self._full_text_queue_sem = Semaphore()
        self._text_queue = Queue()
        self._get_from_text_queue_lock = Lock()
        
        self._dist_queue = Queue()

        self._final_index_queue_sem = Semaphore()
        
        self._num_workers = 0
        self._n_queue_factor = 2
    
    def set_n_queue_factor(self, new_factor:int):
        if type(new_factor) != int:
            raise TypeError('new_factor should be an int!')

        self._n_queue_factor = new_factor
    
    def index_multiprocess(self, num_workers:int = 1):
        """
        There is always 3 proccess at minimum. num_workers set only the number of processes that tokenize 
        the texts. There is another one for reading the files and another one for indexing. 
        """
        self._num_workers = num_workers
        reading_proccess = Process(target=self._get_from_corpus_files, args=())

        final_index_queue = Queue()
        indexing_proccess = Process(target=self._index_text, args=(final_index_queue,))  
        tokenizing_proccesses = []

        for worker_id in range(1, num_workers+1):
            tokenizing_proccesses.append(Process(target=self._tokenize_text, args=()))

        reading_proccess.start()
        [proc.start() for proc in tokenizing_proccesses]
        indexing_proccess.start()
        
        reading_proccess.join()
        [proc.join() for proc in tokenizing_proccesses]

        self._final_index_queue_sem.acquire()
        self._index = final_index_queue.get()
        indexing_proccess.join()
    
    def _get_from_corpus_files(self):
        MAX_DOC = 200
        DOCS_TO_ADD_AS_NEEDED = self._num_workers*self._n_queue_factor
        total_docs_added = 0
        docs_added_to_queue = 0 

        for file in self._corpus_dir_path.glob('*.warc.gz.kaggle'):
            with open(file, 'rb') as stream:
                for record in ArchiveIterator(stream):
                    if docs_added_to_queue == 0:
                        self._empty_text_queue_sem.acquire()

                    text = record.raw_stream.read().decode().strip()
                    total_docs_added += 1
                    docs_added_to_queue += 1
                    self._text_queue.put(text)

                    if total_docs_added == MAX_DOC:
                        if docs_added_to_queue != 0:
                            self._full_text_queue_sem.release()
                            docs_added_to_queue = 0
                        break

                    if docs_added_to_queue == DOCS_TO_ADD_AS_NEEDED:
                        docs_added_to_queue = 0
                        self._full_text_queue_sem.release()
                
            if total_docs_added == MAX_DOC:
                break
        
        #As we use a Queue, we must wait for the queue to be empty to add
        # self._num_workers STOP_FLAGS 
        self._empty_text_queue_sem.acquire()
        for _ in range(self._num_workers):
            self._text_queue.put(Indexer.STOP_FLAG)
        self._full_text_queue_sem.release()

    def _index_text(self, final_index_queue:Queue):
        index = Index()
        doc_id = 1
        while True:

            dist = self._dist_queue.get()
            
            if dist == Indexer.STOP_FLAG:
                break
            else:
                index.add_from_distribuition(dist, doc_id)
                doc_id += 1
        
        final_index_queue.put(index)
        self._final_index_queue_sem.release()
    
    def _tokenize_text(self):
        while True:
            
            self._get_from_text_queue_lock.acquire()
            if self._text_queue.empty():
                self._empty_text_queue_sem.release()
                self._full_text_queue_sem.acquire()

            text_from_corpus = self._text_queue.get()
            self._get_from_text_queue_lock.release()

            if text_from_corpus == Indexer.STOP_FLAG:
                self._dist_queue.put(Indexer.STOP_FLAG)
                break
            elif len(text_from_corpus) > Indexer.MIN_FILE_CHAR_COUNT:

                if TextParser.is_portuguese(text_from_corpus):
                    
                    #Para cada documento, calcular a frequência de palavras e adicionar no index
                    text_distribuition = TextParser.get_distribuition_of(text_from_corpus)
                    self._dist_queue.put(text_distribuition)
    
    def get_postings_dist_as_json(self):
        return self._index.get_postings_dist_as_json()
    
    def get_index_size(self):
        return self._index.size
    
    def clear_index(self):
        self._index = Index()
    
    def reset(self):
        self._index = Index()
        self._empty_text_queue_sem = Semaphore()
        self._full_text_queue_sem = Semaphore()
        self._text_queue = Queue()
        self._get_from_text_queue_lock = Lock()
        self._dist_queue = Queue()
        self._final_index_queue_sem = Semaphore()
        self._num_workers = 0