from index_merger import IndexMerger
from unittest import TestCase, main
import pathlib
import pickle

class TestIndexMerger(TestCase):
    def setUp(self):
        self.sub_indexes_dir = pathlib.Path("subindexes")
        self.sub_indexes_dir.mkdir(exist_ok=True)
        self.index_merger = IndexMerger()

    def create_sub_indexes(self):
        sub_indexes = [
            [
                ('A',[(1, 2), (2, 4), (5, 6)]),
                ('C', [(1, 6), (3, 2), (4, 1)]),
                ('E', [(2, 5), (4, 9),(5, 5)])
            ],
            [
                ('B', [(6,5), (8,6)]),
                ('C', [(7, 6), (8, 5), (10, 3)]),
                ('E', [(7, 2), (9, 2), (10, 1)])
            ]
        ]
        num_sub_indexes = len(sub_indexes)
        sub_indexes_files = [self.sub_indexes_dir / f'sub_index_{i}.pickle' 
                                for i in range(1, num_sub_indexes+1)]
    
        for sub_index_idx, sub_index_file_name in enumerate(sub_indexes_files):
            with open(sub_index_file_name,'wb') as sub_index_file:
                for token_postings_tuple in sub_indexes[sub_index_idx]: 
                    pickle.dump(token_postings_tuple, sub_index_file)
        

    def test_merge_sub_index_files(self):
        self.create_sub_indexes()
        sub_indexes_files = [self.sub_indexes_dir / f'sub_index_{i}.pickle' for i in range(1, 3)]
        merged_file_path = self.sub_indexes_dir / 'final_index/merged_index.pickle'
        self.index_merger.merge_index_file = merged_file_path
        self.index_merger.merge_pickle_files(sub_indexes_files)
        expected_merged_index = [
            ('A',[(1, 2), (2, 4), (5, 6)]),
            ('B', [(6,5), (8,6)]),
            ('C', [(1, 6), (3, 2), (4, 1), (7, 6), (8, 5), (10, 3)]),
            ('E', [(2, 5), (4, 9), (5, 5), (7, 2), (9, 2), (10, 1)])
        ]

        loaded_index = list()
        with open(merged_file_path,'rb') as merged_file:
            while True:
                try:
                    loaded_index.append(pickle.load(merged_file))
                except EOFError:
                    break
        
        self.assertListEqual(expected_merged_index, loaded_index)

        for file in sub_indexes_files:
            if file.exists():
                file.unlink()
        
        if merged_file_path.exists():
            merged_file_path.unlink()
    
    def test_merge_sub_index_files_mem_limit(self):
        self.create_sub_indexes()
        sub_indexes_files = [self.sub_indexes_dir / f'sub_index_{i}.pickle' for i in range(1, 3)]
        merged_file_path = self.sub_indexes_dir / 'final_index/merged_index.pickle'
        max_mem_usage = 10 * 1024 * 1024
        self.index_merger.merge_index_file = merged_file_path
        self.index_merger.merge_pickle_files(sub_indexes_files, max_mem_usage)
        expected_merged_index = [
            ('A',[(1, 2), (2, 4), (5, 6)]),
            ('B', [(6,5), (8,6)]),
            ('C', [(1, 6), (3, 2), (4, 1), (7, 6), (8, 5), (10, 3)]),
            ('E', [(2, 5), (4, 9), (5, 5), (7, 2), (9, 2), (10, 1)])
        ]

        loaded_index = list()
        with open(merged_file_path,'rb') as merged_file:
            while True:
                try:
                    loaded_index.append(pickle.load(merged_file))
                except EOFError:
                    break
        
        self.assertListEqual(expected_merged_index, loaded_index)

        for file in sub_indexes_files:
            if file.exists():
                file.unlink()
        
        if merged_file_path.exists():
            merged_file_path.unlink()
    
    def test_merge_many_levels(self):
        self.create_many_sub_indexes()
        self.index_merger.max_files_opened_at_once = 2

        sub_indexes_files = [self.sub_indexes_dir / f'sub_index_{i}.pickle' for i in range(1, 6)]
        merged_file_path = self.sub_indexes_dir / 'final_index/merged_index.pickle'
        self.index_merger.merge_index_file = merged_file_path
        max_mem_usage = 10 * 1024 * 1024
        
        self.index_merger.merge_pickle_files(sub_indexes_files, max_mem_usage)
        
        expected_merged_index = [
            ('A', [(1, 2), (2, 4), (5, 6), (17, 5), (19, 6)]),
            ('B', [(6, 5), (8, 6), (11, 5), (14, 6)]),
            ('C', [(1, 6), (3, 2), (4, 1), (7, 6), (8, 5), (10, 3), (22, 4), (27, 12)]),
            ('D', [(12, 6), (13, 5), (15, 3), (16, 6), (18, 5), (20, 3)]),
            ('E', [(2, 5), (4, 9), (5, 5), (7, 2), (9, 2), (10, 1), (21, 7), (23, 9)])
        ]

        loaded_index = list()
        with open(merged_file_path,'rb') as merged_file:
            while True:
                try:
                    loaded_index.append(pickle.load(merged_file))
                except EOFError:
                    break
        
        self.assertListEqual(expected_merged_index, loaded_index)

        for file in sub_indexes_files:
            if file.exists():
                file.unlink()
        
        if merged_file_path.exists():
            merged_file_path.unlink()
    
    def create_many_sub_indexes(self):
        sub_indexes = [
            [
                ('A', [(1, 2), (2, 4), (5, 6)]),
                ('C', [(1, 6), (3, 2), (4, 1)]),
                ('E', [(2, 5), (4, 9),(5, 5)])
            ],
            [
                ('B', [(6,5), (8,6)]),
                ('C', [(7, 6), (8, 5), (10, 3)]),
                ('E', [(7, 2), (9, 2), (10, 1)])
            ],
            [
                ('B', [(11,5), (14,6)]),
                ('D', [(12, 6), (13, 5), (15, 3)])
            ],
            [
                ('A', [(17,5), (19,6)]),
                ('D', [(16, 6), (18, 5), (20, 3)])
            ],
            [
                ('C', [(22, 4), (27, 12)]),
                ('E', [(21, 7), (23, 9)])
            ]
        ]

        num_sub_indexes = len(sub_indexes)
        sub_indexes_files = [self.sub_indexes_dir / f'sub_index_{i}.pickle' 
                                for i in range(1, num_sub_indexes+1)]
    
        for sub_index_idx, sub_index_file_name in enumerate(sub_indexes_files):
            with open(sub_index_file_name,'wb') as sub_index_file:
                for token_postings_tuple in sub_indexes[sub_index_idx]: 
                    pickle.dump(token_postings_tuple, sub_index_file)
    
    def test_merge_from_no_starting_level(self):
        self.create_many_sub_indexes_in_dir_of_non_zero_level()
        self.index_merger.max_files_opened_at_once = 2
        self.index_merger.num_levels_run = 1

        first_merged_files_dir = self.sub_indexes_dir / "subindex_merges/merged_index_level_0/"
        first_merged_files_dir.mkdir(parents=True, exist_ok=True)

        sub_indexes_files = [first_merged_files_dir / f'sub_index_{i}.pickle' for i in range(1, 6)]
        merged_file_path = self.sub_indexes_dir / 'final_index/merged_index.pickle'
        self.index_merger.merge_index_file = merged_file_path
        max_mem_usage = 10 * 1024 * 1024
        
        self.index_merger.merge_pickle_files(sub_indexes_files, max_mem_usage)
        
        expected_merged_index = [
            ('A', [(1, 2), (2, 4), (5, 6), (17, 5), (19, 6)]),
            ('B', [(6, 5), (8, 6), (11, 5), (14, 6)]),
            ('C', [(1, 6), (3, 2), (4, 1), (7, 6), (8, 5), (10, 3), (22, 4), (27, 12)]),
            ('D', [(12, 6), (13, 5), (15, 3), (16, 6), (18, 5), (20, 3)]),
            ('E', [(2, 5), (4, 9), (5, 5), (7, 2), (9, 2), (10, 1), (21, 7), (23, 9)])
        ]

        loaded_index = list()
        with open(merged_file_path,'rb') as merged_file:
            while True:
                try:
                    loaded_index.append(pickle.load(merged_file))
                except EOFError:
                    break
        
        self.assertListEqual(expected_merged_index, loaded_index)

        for file in sub_indexes_files:
            if file.exists():
                file.unlink()
        
        if merged_file_path.exists():
            merged_file_path.unlink()
    
    def create_many_sub_indexes_in_dir_of_non_zero_level(self):
        sub_indexes = [
            [
                ('A', [(1, 2), (2, 4), (5, 6)]),
                ('C', [(1, 6), (3, 2), (4, 1)]),
                ('E', [(2, 5), (4, 9),(5, 5)])
            ],
            [
                ('B', [(6,5), (8,6)]),
                ('C', [(7, 6), (8, 5), (10, 3)]),
                ('E', [(7, 2), (9, 2), (10, 1)])
            ],
            [
                ('B', [(11,5), (14,6)]),
                ('D', [(12, 6), (13, 5), (15, 3)])
            ],
            [
                ('A', [(17,5), (19,6)]),
                ('D', [(16, 6), (18, 5), (20, 3)])
            ],
            [
                ('C', [(22, 4), (27, 12)]),
                ('E', [(21, 7), (23, 9)])
            ]
        ]

        num_sub_indexes = len(sub_indexes)
        first_merged_files_dir = self.sub_indexes_dir / "subindex_merges/merged_index_level_0/"
        first_merged_files_dir.mkdir(parents=True, exist_ok=True)

        sub_indexes_files = [first_merged_files_dir / f'sub_index_{i}.pickle' 
                                for i in range(1, num_sub_indexes+1)]
    
        for sub_index_idx, sub_index_file_name in enumerate(sub_indexes_files):
            with open(sub_index_file_name,'wb') as sub_index_file:
                for token_postings_tuple in sub_indexes[sub_index_idx]: 
                    pickle.dump(token_postings_tuple, sub_index_file)
    
    def test_should_merge_another_level(self):
        total_files = 1000
        self.assertTrue(self.index_merger._should_merge_another_level(total_files))

        total_files = 2
        self.assertTrue(self.index_merger._should_merge_another_level(total_files))

        total_files = 1
        self.assertFalse(self.index_merger._should_merge_another_level(total_files))

    def test_is_last_level(self):
        total_files = 1000
        self.index_merger.max_files_opened_at_once = 100
        is_last_level = self.index_merger._is_last_level(total_files)
        self.assertFalse(is_last_level)

        total_files = 100
        is_last_level = self.index_merger._is_last_level(total_files)
        self.assertTrue(is_last_level)

        total_files = 4
        self.index_merger.max_files_opened_at_once = 2
        is_last_level = self.index_merger._is_last_level(total_files)
        self.assertFalse(is_last_level)

if __name__ == '__main__':
    main()