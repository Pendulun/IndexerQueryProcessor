from timeit import default_timer as timer
from parserClasses.myparser import *
from indexClasses.indexer import *
from indexClasses.index import *
from mergerClasses.index_merger import *
from multiprocessing import cpu_count

import logging
import argparse
import resource
import json
import sys


MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

def main(my_args):
    """
    Your main calls should be added here
    """
    final_index_dir = pathlib.Path(my_args.index_path).parent
    sub_indexes_dir = final_index_dir / 'sub_indexes'
    sub_indexes_dir.mkdir(parents=True, exist_ok=True)
    indexer = Indexer(my_args.corpus_path, sub_indexes_dir)

    id_to_doc_file = pathlib.Path("id_to_doc") / 'id_to_doc_map.pickle'
    indexer.id_to_doc_file = id_to_doc_file

    max_num_procs = cpu_count()
    num_procs_for_indexing = 1
    if max_num_procs > 1:
        num_procs_for_indexing = max_num_procs - 1

    max_mem_used_indexing = my_args.memory_limit * 0.9
    
    start = timer()
    indexer.index_multiprocess(max_mem_used_indexing, num_procs_for_indexing)
    
    sub_index_files = list(sub_indexes_dir.glob('*.pickle'))
    final_index_path_file = pathlib.Path(my_args.index_path)
    max_mem_used_index_merge = my_args.memory_limit * 0.45 * MEGABYTE

    index_merger = IndexMerger()
    index_merger.max_files_opened_at_once = 100
    index_merger.merge_index_file = my_args.index_path
    index_merger.merge_pickle_files(sub_index_files, max_mem_used_index_merge)

    end = timer()
    total_time = end-start

    index_size, avg_inv_list_size = Index.get_stats_from_file(final_index_path_file)
    run_info = dict()
    run_info['Index Size'] = final_index_path_file.stat().st_size / MEGABYTE
    run_info['Elapsed Time'] = total_time
    run_info['Number of Lists'] = index_size
    run_info['Average List Size'] = avg_inv_list_size

    print(json.dumps(run_info, indent=4, ensure_ascii=False))

def configArgs(parser):
    parser.add_argument(
        '-m',
        dest='memory_limit',
        action='store',
        required=True,
        type=int,
        help='memory available in MB'
    )

    parser.add_argument(
        '-c',
        dest='corpus_path',
        action='store',
        required=True,
        help='path to the corpus files directory'
    )

    parser.add_argument(
        '-i',
        dest='index_path',
        action='store',
        required=True,
        help='path of the index file to be generated'
    )
    return parser

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser = configArgs(parser)
    logging.basicConfig(level=logging.INFO, format='%(process)d-%(processName)s-%(levelname)s-%(message)s',
    filename="log.log", filemode="w")

    my_args = parser.parse_args()

    memory_limit(my_args.memory_limit)
    try:
        main(my_args)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)