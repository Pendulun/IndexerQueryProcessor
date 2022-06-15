from timeit import default_timer as timer
from parserClasses.myparser import *
from indexClasses.indexer import *
from indexClasses.index import *

import logging
import argparse
import resource
import sys

MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

def main(my_args):
    """
    Your main calls should be added here
    """
    indexer = Indexer(my_args.corpus_path, my_args.index_path)
    #Tudo isso levando em consideração a memória utilizada
    times = {}
    NUM_PROCS = [2]
    N_FACTORS = [3]
    NUM_RUNS = 1
    for n_proc in NUM_PROCS:
        for queue_factor in N_FACTORS:
            times.setdefault(n_proc, {})[queue_factor] = list()
            for run_id in range(NUM_RUNS):
                logging.info(f"n_proc {n_proc} queue_factor {queue_factor} Run {run_id}")
                indexer.set_n_queue_factor(queue_factor)
                start = timer()
                indexer.index_multiprocess(n_proc)
                end = timer()
                total_time = end-start
                times[n_proc][queue_factor].append(total_time)
                logging.info(f"Total time: {total_time} segundos")
                logging.info(f"Index size: {indexer.get_index_size()}")
                indexer.reset()
                # logging.info(f"Frequency Dist:\n{indexer.get_postings_dist_as_json()}")
    

    for n_proc in NUM_PROCS:
        for queue_factor in N_FACTORS:
            tempos = times[n_proc][queue_factor]
            mean_time = 0
            if len(tempos) > 0:
                mean_time = sum(tempos)/len(tempos)
            
            logging.info(f"procs: {n_proc} queue-factor {queue_factor} run times: {times[n_proc][queue_factor]} mean {mean_time}")


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