# You can (and must) freely edit this file (add libraries, functions and calls) to implement your query processor
import argparse
import logging

def main(my_args):
    pass

def configArgs(parser):
    parser.add_argument(
        '-i',
        dest='index_path',
        action='store',
        required=True,
        help='path of the index file to be generated'
    )

    parser.add_argument(
        '-q',
        dest='queries_path',
        action='store',
        required=True,
        help='path to a queries file'
    )

    parser.add_argument(
        '-r',
        dest='ranker',
        action='store',
        choices=['BM25', 'TFIDF'],
        required=True,
        help='the ranker'
    )

    
    return parser

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser = configArgs(parser)
    logging.basicConfig(level=logging.INFO, format='%(process)d-%(processName)s-%(levelname)s-%(message)s',
    filename="log.log", filemode="w")

    my_args = parser.parse_args()

    main(my_args)