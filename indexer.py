import sys
import resource
import argparse
import pathlib
import logging
from warcio import ArchiveIterator
from indexClasses.index import *
from parserClasses.myparser import *

MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

def main(my_args):
    """
    Your main calls should be added here
    """
    corpus_dir = pathlib.Path(my_args.corpus_path)

    assert corpus_dir.exists()

    index_dir_path = pathlib.Path(my_args.index_path)
    index_dir_path.mkdir(parents=True, exist_ok=True)

    assert index_dir_path.exists()

    index = Index()

    WARC_FILE_LIMIT = 1
    DOC_PER_FILE_LIMIT = 3
    
    warc_count = 0
    total_doc_count = 0
    for file in corpus_dir.glob('*.warc.gz.kaggle'):
        doc_count = 0
        warc_count+=1

        with open(file, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':

                    text = record.raw_stream.read().decode()
                    logging.info(text[:100])
                    is_portuguese = TextParser.is_portuguese(text)
                    logging.info(f"Is portuguese: {is_portuguese}")
                    if text.strip() != "" and len(text.strip()) > 10 and is_portuguese:
                        text = TextParser.split_on_upper(text)
                        doc_count +=1

                        text_distribuition = TextParser.get_distribuition_of(text)

                        index.add_from_distribuition(text_distribuition, total_doc_count)

                        total_doc_count += 1

                    
                if doc_count == DOC_PER_FILE_LIMIT:
                    break
        
        if warc_count == WARC_FILE_LIMIT:
            break   
        
    logging.info(f"Index size: {index.size}")
    logging.info(f"Index:\n {index.to_json()}")

    #Para cada documento, calcular a frequência de palavras e adicionar no index
    #Tudo isso levando em consideração a memória utilizada

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
    logging.basicConfig(level=logging.INFO, format='%(thread)d-%(threadName)s-%(levelname)s-%(message)s',
    filename="log.log", filemode="w")

    my_args = parser.parse_args()

    memory_limit(my_args.memory_limit)
    try:
        main(my_args)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)


# You CAN (and MUST) FREELY EDIT this file (add libraries, arguments, functions and calls) to implement your indexer
# However, you should respect the memory limitation mechanism and guarantee
# it works correctly with your implementation