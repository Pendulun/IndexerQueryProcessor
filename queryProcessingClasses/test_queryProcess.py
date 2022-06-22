from math import log
from unittest import TestCase, main
from queryProcessingClasses.queryProcess import QueryProcessor
from parserClasses.myparser import TextParser
import pathlib
import pickle
import json

class TestQueryProcessor(TestCase):
    
    def setUp(self):
        self.queries_file_path = pathlib.Path('queryProcessingClasses/queries/queries.txt')
        self.create_queries_file(self.queries_file_path)

        self.index_file_path = pathlib.Path('queryProcessingClasses/fake_index/fake_index.pickle')
        self.create_index_file(self.index_file_path)

        self.query_processor = QueryProcessor()
        self.query_processor.index_file_path = self.index_file_path

    def create_queries_file(self, queries_file_path:pathlib.Path):
        queries_file_dir = queries_file_path.parent
        queries_file_dir.mkdir(parents=True, exist_ok=True)

        if not queries_file_path.exists():
            queries_list = [
                "Melhores animes de 2022",
                "Cruzeiro Esporte Clube",
                "Como ficar rico rápido"
            ]
            try:
                with open(queries_file_path, 'w') as queries_file:
                    for query in queries_list:
                        queries_file.write(f"{query}\n")
            except Exception as e:
                if queries_file_path.exists():
                    queries_file_path.unlink()
                
                raise e
    
    def create_index_file(self, index_file_path:pathlib.Path):
        queries_file_dir = index_file_path.parent
        queries_file_dir.mkdir(parents=True, exist_ok=True)

        if not index_file_path.exists():
            fake_index = sorted([
                (list(TextParser.pre_proccess('Melhores'))[0],
                    [(1, 2), (7, 4)]
                ),
                (list(TextParser.pre_proccess('animes'))[0], 
                    [(3, 5), (7, 4)]
                ),
                (list(TextParser.pre_proccess('2022'))[0], 
                    sorted([(1, 3), (10, 4), (7, 1)])
                ),
                (list(TextParser.pre_proccess('Cruzeiro'))[0], 
                    [(10, 3), (14, 5), (16, 7)]
                ),
                (list(TextParser.pre_proccess('Esporte'))[0],
                    sorted([(15, 7), (10, 2), (17, 4)])
                ),
                (list(TextParser.pre_proccess('Clube'))[0],
                    sorted([(2, 4), (10, 4), (5, 1)])
                ),
                (list(TextParser.pre_proccess('ficar'))[0], 
                    sorted([(21, 10), (2, 5), (1, 10), (7, 4), (13, 15)])
                ),
                (list(TextParser.pre_proccess('rico'))[0],
                    [(1, 5), (10, 1), (13, 3)]
                ),
                (list(TextParser.pre_proccess('rápido'))[0],
                    [(9, 5), (13, 3), (16, 2)]
                ),
            ])
            
            try:
                with open(index_file_path, 'wb') as index_file:
                    [pickle.dump(inverted_list, index_file) for inverted_list in fake_index]
            except Exception as e:
                if index_file_path.exists():
                    index_file_path.unlink()
                
                raise e
    
    def test_can_tokenize_query(self):
        doc_id_to_url_map = set()
        
        doc_ids = list(range(30))
        for doc_id in doc_ids:
            doc_id_to_url_map.add((doc_id, f'url{doc_id}'))
        
        fake_doc_to_url_map_file_path = pathlib.Path('queryProcessingClasses/queries/doc_to_url.map.pickle')
        with open(fake_doc_to_url_map_file_path, 'wb') as fake_doc_to_url_map_file:
            pickle.dump(doc_id_to_url_map, fake_doc_to_url_map_file)
        
        self.query_processor.doc_id_to_url_file_path = fake_doc_to_url_map_file_path

        query_list = list()
        with open(self.queries_file_path, 'r') as queries_file:
            query_list = [query for query in queries_file]

        rankings = self.query_processor.process_queries(query_list)

        #NO ASSERT :/

    def test_convert_doc_id_to_url(self):
        doc_id_to_url_map = set()
        
        doc_ids = [1,6,4,8,2,0]
        for doc_id in doc_ids:
            doc_id_to_url_map.add((doc_id, f'url{doc_id}'))
        
        fake_doc_to_url_map_file_path = pathlib.Path('queryProcessingClasses/queries/doc_to_url.map.pickle')
        with open(fake_doc_to_url_map_file_path, 'wb') as fake_doc_to_url_map_file:
            pickle.dump(doc_id_to_url_map, fake_doc_to_url_map_file)
        
        self.query_processor.doc_id_to_url_file_path = fake_doc_to_url_map_file_path

        my_fake_ranking = [(10, 4), (9, 2), (8, 1)]

        converted_ranking = self.query_processor.convert_ranking_doc_ids_to_urls(my_fake_ranking)

        expected_converted_ranking = [(10, 'url4'), (9, 'url2'), (8, 'url1')]
        self.assertListEqual(expected_converted_ranking, converted_ranking)

        if fake_doc_to_url_map_file_path.exists():
            fake_doc_to_url_map_file_path.unlink()

if __name__ == '__main__':
    main()