# You can (and must) freely edit this file (add libraries, functions and calls) to implement your query processor
import argparse
import logging
from queryProcessingClasses.queryProcess import QueryProcessor
import json

def main(my_args):
    query_processor = QueryProcessor()

    query_processor.index_file_path = my_args.index_path

    queries_list = list()
    with open(my_args.queries_path, 'r') as queries_file:
        queries_list = [query for query in queries_file]
    
    responses = query_processor.process_queries(queries_list)
    for response in responses:
        print(json.dumps(response, ensure_ascii=False, indent=3))

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