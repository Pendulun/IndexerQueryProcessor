from unittest import TestCase, main
from index_merger import IndexMerger
import pathlib
import pickle

class TestIndexMerger(TestCase):
    def setUp(self):
        self.sub_indexes_dir = pathlib.Path("subindexes")
        self.sub_indexes_dir.mkdir(exist_ok=True)
        self.index_merger = IndexMerger()
        self.create_sub_indexes()

    def create_sub_indexes(self):
        first_sub_index = [
            (
                'A',[(1, 2), (2, 4), (5, 6)]
            ),
            (
                'C', [(1, 6), (3, 2), (4, 1)] 
            ),
            (
                'E', [(2, 5), (4, 9),(5, 5)]
            )
        ]

        second_sub_index = [
            (
                'B', [(6,5), (8,6)],
            ),
            (
                'C', [(7, 6), (8, 5), (10, 3)],
            ),
            (
                'E', [(7, 2), (9, 2), (10, 1)]
            )
        ]
        sub_index_1 = self.sub_indexes_dir / 'sub_index_1.pickle'
        sub_index_2 = self.sub_indexes_dir / 'sub_index_2.pickle'

        with open(sub_index_1,'wb') as sub_index_1_file:
            for token_postings_tuple in first_sub_index: 
                pickle.dump(token_postings_tuple, sub_index_1_file)
        
        with open(sub_index_2,'wb') as sub_index_2_file:
            for token_postings_tuple in second_sub_index: 
                pickle.dump(token_postings_tuple, sub_index_2_file)
        

    def test_merge_sub_index_files(self):
        file_1 = self.sub_indexes_dir / 'sub_index_1.pickle'
        file_2 = self.sub_indexes_dir / 'sub_index_2.pickle'
        merged_file_path = self.sub_indexes_dir / 'merged_index.pickle'
        self.index_merger.merge_pickle_files_to([file_1, file_2], merged_file_path)
        expected_merged_index = [
            (
                'A',[(1, 2), (2, 4), (5, 6)]
            ),
            (
                'B', [(6,5), (8,6)],
            ),
            (
                'C', [(1, 6), (3, 2), (4, 1), (7, 6), (8, 5), (10, 3)] 
            ),
            (
                'E', [(2, 5), (4, 9), (5, 5), (7, 2), (9, 2), (10, 1)]
            )
        ]

        loaded_index = list()
        with open(merged_file_path,'rb') as merged_file:
            while True:
                try:
                    loaded_index.append(pickle.load(merged_file))
                except EOFError:
                    break
        
        self.assertListEqual(expected_merged_index, loaded_index)
    
    def test_merge_sub_index_files_mem_limit(self):
        file_1 = self.sub_indexes_dir / 'sub_index_1.pickle'
        file_2 = self.sub_indexes_dir / 'sub_index_2.pickle'
        merged_file_path = self.sub_indexes_dir / 'merged_index.pickle'
        self.index_merger.merge_pickle_files_to([file_1, file_2], merged_file_path, 10 * 1024 * 1024)
        expected_merged_index = [
            (
                'A',[(1, 2), (2, 4), (5, 6)]
            ),
            (
                'B', [(6,5), (8,6)],
            ),
            (
                'C', [(1, 6), (3, 2), (4, 1), (7, 6), (8, 5), (10, 3)] 
            ),
            (
                'E', [(2, 5), (4, 9), (5, 5), (7, 2), (9, 2), (10, 1)]
            )
        ]

        loaded_index = list()
        with open(merged_file_path,'rb') as merged_file:
            while True:
                try:
                    loaded_index.append(pickle.load(merged_file))
                except EOFError:
                    break
        
        self.assertListEqual(expected_merged_index, loaded_index)

if __name__ == '__main__':
    main()