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

        self._global_doc_id_lock = Lock()
        self._global_doc_id = Value('i', 1)
    
    def set_n_queue_factor(self, new_factor:int):
        if type(new_factor) != int:
            raise TypeError('new_factor should be an int!')

        self._n_queue_factor = new_factor
    
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
        MAX_DOCS_TO_CONSIDER_PER_WARC_FILE = 100
        index = Index()

        warc_file_count = 0
        for file in warc_files:
            warc_file_count += 1
            doc_added = 0
            with open(file, 'rb') as stream:
                for record in ArchiveIterator(stream):
                    text_from_doc = record.raw_stream.read().decode().strip()
                    if len(text_from_doc) > Indexer.MIN_FILE_CHAR_COUNT:
                        
                        #Para cada documento, calcular a frequência de palavras e adicionar no index
                        text_distribuition = TextParser.get_distribuition_of(text_from_doc)
                        
                        self._global_doc_id_lock.acquire()
                        curr_doc_id = self._global_doc_id.value
                        self._global_doc_id.value += 1
                        self._global_doc_id_lock.release()

                        index.add_from_distribuition(text_distribuition, curr_doc_id)
                        #print(f"Proc {proc_id} added doc {curr_doc_id} from warc_file {warc_file_count} of {len(warc_files)}")

                        doc_added += 1

                        if doc_added >= MAX_DOCS_TO_CONSIDER_PER_WARC_FILE:
                            break
        
        print(f"PROC {proc_id} OUT")
    
    def reset(self):
        self._global_doc_id.value = 0