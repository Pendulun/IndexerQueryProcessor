import pickle
import psutil
import pathlib
import os

class IndexMerger():
    
    MEGABYTE = 1024 * 1024

    def merge_pickle_files_to(self, file_list:list, merged_file_path:str, max_mem_usage:int = 700 * MEGABYTE):

        merge_index_file = pathlib.Path(merged_file_path)
        if merge_index_file.exists():
            merge_index_file.unlink()

        open_files_list = list()
        curr_token_per_file = {}
        merged_index = {}
        curr_postings_from_files = {}
        curr_files_read = set()
        try:
            for file_name in file_list:
                curr_file = open(file_name, 'rb')
                open_files_list.append(curr_file)
            
            while len(open_files_list) != 0:
                #Lê linha de quem for necessário
                for file_idx, opened_file in enumerate(open_files_list):
                    if file_idx not in curr_files_read:
                        try:
                            line = pickle.load(opened_file)
                        except EOFError:
                            opened_file_instance = open_files_list[file_idx]
                            open_files_list.remove(opened_file_instance)
                            opened_file_instance.close()
                        else:
                            curr_postings_from_files[file_idx] = line[1] 
                            curr_token_per_file.setdefault(line[0], []).append(file_idx)
                            curr_files_read.add(file_idx)
                
                if len(curr_token_per_file) > 0:
                    #Pega os arquivos associados ao menor token
                    curr_token, idx_files_associated = sorted(curr_token_per_file.items())[0]

                    #Juntar os seus valores
                    all_token_postings = list()
                    for fileidx in idx_files_associated:
                        all_token_postings.extend(curr_postings_from_files[fileidx])
                        curr_postings_from_files[fileidx] = []
                        curr_files_read.remove(fileidx)
                    
                    #Completar o index
                    merged_index[curr_token] = sorted(all_token_postings)

                    curr_mem_usage = psutil.Process(os.getpid()).memory_info().rss
                    if curr_mem_usage >= max_mem_usage:
                        self._save_and_clear_index(merged_index, merge_index_file)

                    #Deletar do curr_token_per_file
                    del curr_token_per_file[curr_token]

        except Exception as e:
            print(e)
            raise e
        finally:
            for open_file in open_files_list:
                open_file.close()

        self._save_and_clear_index(merged_index, merge_index_file)
    
    def _save_and_clear_index(self, index:dict, file):
        with open(file, 'ab') as merged_index_file:
            for token, postings in index.items():
                pickle.dump((token, postings), merged_index_file)
        
        index.clear()