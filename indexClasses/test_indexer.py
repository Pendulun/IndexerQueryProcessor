from unittest import TestCase, main
from indexClasses.indexer import Indexer
from indexClasses.index import Index
import pathlib

class TestIndexer(TestCase):
    
    def setUp(self):
        self.indexer = Indexer('/mnt/c/Users/User/Downloads/RECINFO', 'index')
        self.test_dir = pathlib.Path('test_utils')
        self.test_dir.mkdir(exist_ok=True)
        assert self.test_dir

    def test_load_id_to_doc_map(self):
        original_map = set()
        original_map.add((1, 'test1'))
        original_map.add((2, 'test2'))
        original_map.add((3, 'test3'))

        (self.test_dir/ "test_load_id_to_doc_map.pickle").unlink()
        self.indexer.write_id_to_doc_map_to(original_map, self.test_dir/ "test_load_id_to_doc_map.pickle")

        loaded_set = self.indexer.load_id_to_doc_map_from(self.test_dir/ "test_load_id_to_doc_map.pickle")

        self.assertSetEqual(original_map, loaded_set)
    
    def test_load_multiple_id_to_doc_map(self):
        original_map = set()
        original_map.add((1, 'test1'))
        original_map.add((2, 'test2'))
        original_map.add((3, 'test3'))

        complete_original_set = set()
        complete_original_set.add((1, 'test1'))
        complete_original_set.add((2, 'test2'))
        complete_original_set.add((3, 'test3'))

        (self.test_dir/ "test_load_multiple_id_to_doc_map.pickle").unlink()

        self.indexer.write_id_to_doc_map_to(original_map, self.test_dir/ "test_load_multiple_id_to_doc_map.pickle")

        original_map.clear()

        original_map.add((4, 'test4'))
        complete_original_set.add((4, 'test4'))

        self.indexer.write_id_to_doc_map_to(original_map, self.test_dir/ "test_load_id_to_doc_map.pickle")

        loaded_set = self.indexer.load_id_to_doc_map_from(self.test_dir/ "test_load_id_to_doc_map.pickle")

        self.assertSetEqual(complete_original_set, loaded_set)

if __name__ == '__main__':
    main()