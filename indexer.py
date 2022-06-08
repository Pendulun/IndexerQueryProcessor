import sys
import resource
import argparse
import pathlib
from warcio import ArchiveIterator

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
    
    #Ler um arquivo warc

    for file in corpus_dir.glob('*.warc.gz.kaggle'):

        with open(file, 'rb') as stream:
            
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    texto = record.raw_stream.read().decode()

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