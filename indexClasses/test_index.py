from unittest import TestCase, main
from index import Index, PostingTuple
import pathlib

class TestIndex(TestCase):

    def setUp(self):
        self.index = Index()

    def test_can_add_to_index(self):
        token = 'test'
        doc_id = 1
        frequency = 7
        posting = PostingTuple(doc_id, frequency)

        self.index.add(token, posting)

        self.assertTrue(self.index.has_entry(token, posting.doc_id))
    
    def test_can_add_multiple_to_index(self):
        tokens = ['test1', 'test2', 'test3']
        doc_ids = [1, 1, 3]
        frequencies = [7, 4, 9]

        for posting_id in range(len(tokens)):
            posting = PostingTuple(doc_ids[posting_id], frequencies[posting_id])

            self.index.add(tokens[posting_id], posting)
        

        for posting_id in range(len(tokens)):
            self.assertTrue(self.index.has_entry(tokens[posting_id], doc_ids[posting_id]))
        
        self.assertTupleEqual(self.index.size, (3, 3))
    
    def test_add_same_doc_for_token(self):
        token = 'test1'
        doc_id = 1
        frequencies = [1, 2]

        posting = PostingTuple(doc_id, frequencies[0])
        self.index.add(token, posting)

        posting = PostingTuple(doc_id, frequencies[1])
        self.index.add(token, posting)

        self.assertEqual(self.index.get_freq_of_doc_for_token(doc_id, token), 3)
        self.assertEqual(self.index.get_num_postings_for(token), 1)
    
    def test_doesnt_have_posting_without_token(self):
        self.assertFalse(self.index.has_entry('test', 1))
    
    def test_doesnt_have_posting_with_token(self):
        token = 'test'
        doc_id = 1
        frequency = 3

        posting = PostingTuple(doc_id, frequency)
        self.index.add(token, posting)

        self.assertFalse(self.index.has_entry('test', 2))
    
    def test_get_posting(self):
        self.index = self.add_a_bunch_of_postings(self.index)
        self.assertEqual(self.index.get_posting('test1', 7), PostingTuple(7, 7))
    
    def test_get_entire_index(self):
        self.index = self.add_a_bunch_of_postings(self.index)

        expected_dict = {
            'test1':{
                0:3,
                4:5,
                7:7
            },
            'test2':{
                4:1,
                100:2,
                105:3
            }
        }

        self.assertDictEqual(self.index.get_entire_index(), expected_dict)
    
    def test_get_postings_dist(self):
        self.index = self.add_a_bunch_of_postings(self.index)

        expected_dict = {3: 2, 5: 1, 7: 1, 1: 1, 2: 1}

        self.assertDictEqual(self.index.get_postings_dist(), expected_dict)

    def test_update_index_from_list_of_tuples(self):
        self.index = self.add_a_bunch_of_postings(self.index)
        list_of_tuples = [
            ('test30',[
                (1,7),
                (4,9)   
            ]),
            ('test31', [
                (9,10),
                (11, 12)
            ])
        ]

        self.index.update_from_list_of_tuples(list_of_tuples)

        expected_index = Index()
        expected_index =  self.add_a_bunch_of_postings(expected_index)
        for token, postings_list in list_of_tuples:
            for posting in postings_list:
                expected_index.add(token, PostingTuple(posting[0], posting[1]))
    
        self.assertDictEqual(expected_index.get_entire_index(), self.index.get_entire_index())

    def add_a_bunch_of_postings(self, index: Index) -> Index:
        tokens_postings = {
            'test1':{
                'doc_ids': [0, 4, 7],
                'freq': [3, 5, 7]
            },
            'test2':{
                'doc_ids': [4, 100, 105],
                'freq': [1,2,3]
            }
        }

        for token, posting_info in tokens_postings.items():
            for post_id in range(len(posting_info['doc_ids'])):
                new_posting = PostingTuple(posting_info['doc_ids'][post_id], posting_info['freq'][post_id])
                index.add(token, new_posting)
        
        return index
    
    def test_dont_get_posting(self):
        posting = PostingTuple(1,1)
        self.index.add('test1', posting)
        self.assertEqual(self.index.get_posting('test1', 2), None)
        self.assertEqual(self.index.get_posting('test2', 1), None)
    
    def test_add_from_distribuition(self):
        doc_id = 7
        distribuition = {
            'test1': 5,
            'test2': 3,
            'test3': 8,
            'test4': 9
        }

        self.index.add_from_distribuition(distribuition, doc_id)
        for token in distribuition.keys():
            self.assertTrue(self.index.has_entry(token, doc_id))
        
        self.assertTupleEqual(self.index.size, (4, 4))
    
    def test_load_index(self):
        test_dir = pathlib.Path('test_utils')
        test_dir.mkdir(exist_ok=True)
        assert test_dir.exists()
        
        original_index = Index()

        list_of_tuples = [
            ('test30',[
                (1,7),
                (4,9)   
            ]),
            ('test31', [
                (9,10),
                (11, 12)
            ])
        ]
        
        test_file = (test_dir/ "test_load_index.pickle") 
        if test_file.exists():
            test_file.unlink()

        original_index.update_from_list_of_tuples(list_of_tuples)
        original_index.save_to_pickle(test_file)

        loaded_index = Index()
        loaded_index.load_index_from(test_file)

        self.assertDictEqual(original_index.get_entire_index(), loaded_index.get_entire_index())

if __name__ == '__main__':
    main()