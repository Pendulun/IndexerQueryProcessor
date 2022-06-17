from multiprocessing import Process, Queue, Semaphore, Lock, Value
from parserClasses.myparser import TextParser
from indexClasses.index import Index
from warcio import ArchiveIterator
import pathlib
import time

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

        self._global_doc_id = Value('i', 1)
    
    def set_n_queue_factor(self, new_factor:int):
        if type(new_factor) != int:
            raise TypeError('new_factor should be an int!')

        self._n_queue_factor = new_factor
    
    # def index_multiprocess(self, num_workers:int = 1):
    #     """
    #     There is always 3 proccess at minimum. num_workers set only the number of processes that tokenize 
    #     the texts. There is another one for reading the files and another one for indexing. 
    #     """
    #     self._num_workers = num_workers
    #     reading_proccess = Process(target=self._get_from_corpus_files, args=())

    #     final_index_queue = Queue()
    #     indexing_proccess = Process(target=self._index_text, args=(final_index_queue,))  
    #     tokenizing_proccesses = []

    #     for worker_id in range(1, num_workers+1):
    #         tokenizing_proccesses.append(Process(target=self._tokenize_text, args=()))

    #     reading_proccess.start()
    #     [proc.start() for proc in tokenizing_proccesses]
    #     indexing_proccess.start()
        
    #     reading_proccess.join()
    #     [proc.join() for proc in tokenizing_proccesses]

    #     self._final_index_queue_sem.acquire()
    #     self._index = final_index_queue.get()
    #     print("GOT FINAL INDEX")
    #     indexing_proccess.join()
    
    def index_multiprocess(self, num_workers:int = 1):
        warc_files_per_proc = self._get_warc_files_per_proc(num_workers)

        procs = []
        for proc_id, warc_files in warc_files_per_proc.items():
            procs.append(Process(target=self.create_subindex, args=(warc_files, proc_id)))
        
        [proc.start() for proc in procs]

        [proc.join() for proc in procs]

    
    def _get_warc_files_per_proc(self, num_procs:int) -> dict:
        warc_files = list(self._corpus_dir_path.glob('*.warc.gz.kaggle'))

        proc_block_size = len(warc_files)//num_procs

        warc_files_per_proc = dict()
        for proc_idx in range(num_procs):
            first_warc_file_idx = proc_idx*proc_block_size
            last_warc_file_idx = first_warc_file_idx + proc_block_size
            warc_files_per_proc[proc_idx] = warc_files[first_warc_file_idx:last_warc_file_idx+1]
        
        #leftover docs
        if proc_block_size * num_procs != len(warc_files):
            left_over_warc_files = warc_files[proc_block_size * num_procs:]
            for proc_idx, warc_file in enumerate(left_over_warc_files):
                warc_files_per_proc[proc_idx].append(warc_file)
        
        return warc_files_per_proc
    
    def create_subindex(self, warc_files:list, proc_id:int):
        MAX_DOCS_TO_CONSIDER_PER_WARC_FILE = 30
        index = Index()

        warc_file_count = 0
        for file in warc_files:
            doc_added = 0
            with open(file, 'rb') as stream:
                for record in ArchiveIterator(stream):
                    text_from_doc = record.raw_stream.read().decode().strip()
                    if len(text_from_doc) > Indexer.MIN_FILE_CHAR_COUNT:
                        
                        #Para cada documento, calcular a frequência de palavras e adicionar no index
                        text_distribuition = TextParser.get_distribuition_of(text_from_doc)
                        curr_doc_id = self._global_doc_id.value
                        index.add_from_distribuition(text_distribuition, curr_doc_id)
                        #print(f"Proc {proc_id} added doc {curr_doc_id} from warc_file {warc_file_count} of {len(warc_files)}")

                        doc_added += 1
                        self._global_doc_id.value += 1

                        if doc_added >= MAX_DOCS_TO_CONSIDER_PER_WARC_FILE:
                            break
            
            warc_file_count += 1
        
        print(f"PROC {proc_id} OUT")

    def _get_from_corpus_files(self):
        MAX_DOC = 1000
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
        print("READER OUT")

    def _index_text(self, final_index_queue:Queue):
        index = Index()
        
        #mem checking stuff
        index_total_mem_size = index.mem_size()
        mem_diff_every_add = 0
        check_mem_on_add = 6
        last_check_mem_on_add = 0
        should_sub_one = True

        doc_id = 1
        
        # mem_diffs = list()
        # total_mems = list()
        # real_mems = list()
        while True:

            dist = self._dist_queue.get()
            
            if dist == Indexer.STOP_FLAG:
                break
            else:
                print(f"Indexando {doc_id}")
                index.add_from_distribuition(dist, doc_id)
                
                #check for mem
                
                # if doc_id == last_check_mem_on_add + 1:
                #     mem_diff_every_add = index.mem_size() - index_total_mem_size
                # elif doc_id == check_mem_on_add:
                #     index_total_mem_size = index.mem_size()
                #     last_check_mem_on_add = check_mem_on_add
                #     check_mem_on_add = check_mem_on_add*2
                #     if should_sub_one:
                #         check_mem_on_add -= 1
                    
                #     should_sub_one = not should_sub_one
                
                # if doc_id != last_check_mem_on_add:
                #     index_total_mem_size += mem_diff_every_add

                if doc_id == check_mem_on_add:
                    index_total_mem_size = index.mem_size()
                    last_check_mem_on_add = check_mem_on_add
                    check_mem_on_add = check_mem_on_add*2
                    if should_sub_one:
                        check_mem_on_add -= 1
                    
                    should_sub_one = not should_sub_one
                else:
                    if doc_id == last_check_mem_on_add + 1:
                        mem_diff_every_add = index.mem_size() - index_total_mem_size

                    index_total_mem_size += mem_diff_every_add
                
                # real_mem = index.mem_size()
                # mem_diffs.append(index_total_mem_size/real_mem)
                # real_mems.append(real_mem)
                # total_mems.append(index_total_mem_size)

                doc_id += 1
        
        print(f"index expected mem size: {index_total_mem_size}")
        print(f"'True' mem size: {index.mem_size()}")
        
        # my_time = int(time.time())
        # with open(f"mem_diffs-{my_time}.txt",'w') as mem_diffs_file:
        #     mem_diffs_file.write(str(mem_diffs))
        
        # with open(f"total_mems-{my_time}.txt",'w') as total_mems_file:
        #     total_mems_file.write(str(total_mems))
        
        # with open(f"real_mems-{my_time}.txt",'w') as real_mems_file:
        #     real_mems_file.write(str(real_mems))

        final_index_queue.put(index)
        self._final_index_queue_sem.release()
        print("INDEXER OUT")
    
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

                # if TextParser.is_portuguese(text_from_corpus):
                    
                #Para cada documento, calcular a frequência de palavras e adicionar no index
                text_distribuition = TextParser.get_distribuition_of(text_from_corpus)
                self._dist_queue.put(text_distribuition)
            else:
                print("NÃO PASSOU")
        
        print("TOKENIZER OUT")
    
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
        self._global_doc_id.value = 0