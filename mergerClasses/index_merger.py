from multiprocessing import Pool, cpu_count
from timeit import default_timer as timer
import pathlib
import pickle
import psutil
import os

class IndexMerger():
    
    MEGABYTE = 1024 * 1024
    LEVEL_DIR_FORMATER = "subindex_merges/merged_index_level_{}/"

    def __init__(self):
        self._max_files_opened_at_once = 100
        self._merge_index_file = "" #Path
        self._curr_fake_level_for_dir_name = 0
    
    @property
    def max_files_opened_at_once(self):
        """
        The maximum amount of files to open at once
        """
        return self._max_files_opened_at_once
    
    @max_files_opened_at_once.setter
    def max_files_opened_at_once(self, new_max:int):
        if type(new_max) != int:
            raise TypeError("new_max should be an int!")
        
        MIN_FILES_TO_OPEN = 2
        if new_max < MIN_FILES_TO_OPEN:
            raise ValueError(f"new_max should not be less than {MIN_FILES_TO_OPEN}")

        self._max_files_opened_at_once = new_max
    
    @property
    def merge_index_file(self):
        """
        The final index path
        """
        return self._merge_index_file
    
    @merge_index_file.setter
    def merge_index_file(self, new_merge_index_file:str):
        if isinstance(new_merge_index_file, str):
            self._merge_index_file = pathlib.Path(new_merge_index_file)
        elif isinstance(new_merge_index_file, pathlib.Path):
            self._merge_index_file = new_merge_index_file
        else:
            raise TypeError("new_merge_index_file should be a str or pathlib.Path")
    
    @property
    def num_levels_run(self):
        """
        The number of levels already run
        """
        return self._curr_fake_level_for_dir_name

    @num_levels_run.setter
    def num_levels_run(self, new_starting_merge_level):
        if type(new_starting_merge_level) != int or new_starting_merge_level < 0:
            raise TypeError("new_starting_merge_level should be a non negative integer!")
        
        self._curr_fake_level_for_dir_name = new_starting_merge_level

    def merge_pickle_files(self, file_list:list, max_mem_usage:int = 700 * MEGABYTE):
        self._clear_and_create_parents(self._merge_index_file)

        total_files = len(file_list)
        
        curr_real_level = 0
        print("STARTING MERGE")
        while self._should_merge_another_level(total_files):
            print(f"CURR Level real: {curr_real_level} fake: {self._curr_fake_level_for_dir_name}")
            start = timer()
            self._merge_level_files(file_list, max_mem_usage, self._curr_fake_level_for_dir_name)
            end = timer()
            print(f"End level {curr_real_level} in {end-start} seconds")
            
            produced_merged_indexes_dir = ""
            if self._is_last_level(total_files):
                total_files = 1
            else:    
                produced_merged_indexes_dir = pathlib.Path(IndexMerger.LEVEL_DIR_FORMATER.format(self._curr_fake_level_for_dir_name))
            
                file_list = list(produced_merged_indexes_dir.glob('*.pickle'))
                total_files = len(file_list)
                curr_real_level += 1
                self._curr_fake_level_for_dir_name += 1
        
        print("MERGE ENDED")
        
    def _clear_and_create_parents(self, merge_index_file:pathlib.Path):
        if merge_index_file.exists():
            merge_index_file.unlink()
        merge_index_file.parent.mkdir(parents=True, exist_ok=True)
    
    def _should_merge_another_level(self, total_files:int):
        return total_files > 1

    def _merge_level_files(self, file_list:list, max_mem_usage:int, curr_fake_level_for_dir_name:int):
        
        self._clear_level_dir(curr_fake_level_for_dir_name)
        
        total_files = len(file_list)
        file_range_list = list()
        print(f"Num arquivos esse level: {len(file_list)}")
        initial_idx = 0
        final_idx = initial_idx + self._max_files_opened_at_once
        
        while initial_idx < total_files:
            file_range_list.append(file_list[initial_idx:final_idx].copy())
            initial_idx = final_idx
            final_idx += self._max_files_opened_at_once
        
        file_list.clear()
        print(f"Num blocos este level: {len(file_range_list)}")
        is_last_level = self._is_last_level(total_files)

        mem_usage_per_proc = max_mem_usage/cpu_count()

        procs_args = [(is_last_level, mem_usage_per_proc, curr_fake_level_for_dir_name, files, files_block_id) 
                            for files_block_id, files in enumerate(file_range_list)]
        with Pool(cpu_count()) as proc_pool:
            proc_pool.starmap(self._merge_files_multi_process, procs_args)

    def _clear_level_dir(self, curr_fake_level_for_dir_name):
        curr_level_dir = pathlib.Path(IndexMerger.LEVEL_DIR_FORMATER.format(curr_fake_level_for_dir_name))
        [file.unlink() for file in curr_level_dir.glob("*.pickle")]
    
    def _merge_files_multi_process(self, is_last_level:bool, max_mem_usage:int, curr_fake_level_for_dir_name:int, file_list:list, block_id:int):
        if is_last_level:
            self._merge_files(file_list, max_mem_usage, curr_fake_level_for_dir_name, is_last_level)
        else:
            self._merge_files(file_list, max_mem_usage, curr_fake_level_for_dir_name, is_last_level, block_id=block_id)
    
    def _is_last_level(self, total_files:int) -> bool:
        return total_files <= self._max_files_opened_at_once
    
    def _merge_files(self, file_list:list, max_mem_usage = 700 * MEGABYTE, level:int = 0, is_last_level:bool = False, block_id:int = 0):
        open_files_list = list()
        curr_token_per_file = dict()
        merged_index = dict()
        curr_postings_from_files = dict()
        curr_files_read = set()

        merged_index_file = 0
        if is_last_level:
            merged_index_file = self._merge_index_file
        else:
            level_dir_path = pathlib.Path(IndexMerger.LEVEL_DIR_FORMATER.format(level))
            level_dir_path.mkdir(parents=True, exist_ok=True)
            merged_index_file = level_dir_path / f"merged_index_{block_id}.pickle"
            print(f"Creating merged file at: {merged_index_file}")
            if merged_index_file.exists():
                merged_index_file.unlink()

        try:
           
            open_files_list = self._get_open_files_list(file_list, open_files_list)
            
            while len(open_files_list) != 0:
                open_files_list, curr_token_per_file, curr_postings_from_files, curr_files_read = self._read_lines_if_needed(
                                                open_files_list, curr_token_per_file, curr_postings_from_files, curr_files_read)
                
                if len(curr_token_per_file) > 0:
                    curr_token, idx_files_associated = self._get_next_token_and_files_to_process(curr_token_per_file)
                    all_token_postings = self._merge_token_postings(curr_postings_from_files, curr_files_read, idx_files_associated)
                    
                    merged_index[curr_token] = all_token_postings

                    curr_mem_usage = psutil.Process(os.getpid()).memory_info().rss
                    if curr_mem_usage >= max_mem_usage:
                        self._save_and_clear_index(merged_index, merged_index_file)

                    #Deletar do curr_token_per_file
                    del curr_token_per_file[curr_token]

        except Exception as e:
            print(e)
            raise e
        finally:
            for open_file in open_files_list:
                open_file.close()

        self._save_and_clear_index(merged_index, merged_index_file)

    def _merge_token_postings(self, curr_postings_from_files:dict, curr_files_read:set, idx_files_associated_with_token:list):
        all_token_postings = list()
        for fileidx in idx_files_associated_with_token:
            all_token_postings.extend(curr_postings_from_files[fileidx])
            curr_postings_from_files[fileidx] = []
            curr_files_read.remove(fileidx)
        
        sorted_postings = sorted(all_token_postings)
        return sorted_postings

    def _get_next_token_and_files_to_process(self, curr_token_per_file:dict) -> tuple:
        return sorted(curr_token_per_file.items())[0]

    def _read_lines_if_needed(self, open_files_list, curr_token_per_file, curr_postings_from_files, curr_files_read):
        for file_idx, opened_file in enumerate(open_files_list):
            if file_idx not in curr_files_read:
                try:
                    line = pickle.load(opened_file)
                except EOFError:
                    self._close_file(open_files_list, file_idx)
                else:
                    self._add_curr_token_postings_from_file(curr_token_per_file, curr_postings_from_files, curr_files_read, file_idx, line)
        
        return open_files_list, curr_token_per_file, curr_postings_from_files, curr_files_read

    def _close_file(self, open_files_list, file_idx):
        opened_file_instance = open_files_list[file_idx]
        open_files_list.remove(opened_file_instance)
        opened_file_instance.close()
    
    def _add_curr_token_postings_from_file(self, curr_token_per_file, curr_postings_from_files, curr_files_read, file_idx, line):
        curr_postings_from_files[file_idx] = line[1]
        curr_token_per_file.setdefault(line[0], []).append(file_idx)
        curr_files_read.add(file_idx)

    def _get_open_files_list(self, file_list:list, open_files_list:list):
        for file_name in file_list:
            curr_file = open(file_name, 'rb')
            open_files_list.append(curr_file)
        
        return open_files_list
    
    def _save_and_clear_index(self, index:dict, file:str):
        with open(file, 'ab') as merged_index_file:
            for token, postings in index.items():
                pickle.dump((token, postings), merged_index_file)
        
        index.clear()