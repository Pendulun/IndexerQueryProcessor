from unittest import TestCase, main
from index import Index, PostingTuple, InvertedList

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
    
    def test_add_same_doc_for_token(self):
        token = 'test1'
        doc_id = 1
        frequencies = [1, 2]

        posting = PostingTuple(doc_id, frequencies[0])
        self.index.add(token, posting)

        posting = PostingTuple(doc_id, frequencies[1])
        self.index.add(token, posting)

        self.assertEqual(self.index.get_freq_of_doc_for_token(doc_id, token), 3)
    
    def test_has_posting_after_checkpoint(self):
        token = 'test1'
        max_doc_id = InvertedList.CHECKPOINTS_EVERY + 2
        for doc_id in range(max_doc_id):
            frequency = doc_id
            posting = PostingTuple(doc_id, frequency)
            self.index.add(token, posting)
    
        self.assertTrue(self.index.has_entry(token, max_doc_id-1))
    
    def test_doesnt_have_posting_without_token(self):
        self.assertFalse(self.index.has_entry('test', 1))
    
    def test_doesnt_have_posting_with_token(self):
        token = 'test'
        doc_id = 1
        frequency = 3

        posting = PostingTuple(doc_id, frequency)
        self.index.add(token, posting)

        self.assertFalse(self.index.has_entry('test', 2))
    
    def test_delta_steps_one_token(self):
        token = 'test1'
        doc_ids = [1, 4, 7]
        frequencies = [3, 5, 7]

        for posting_id in range(len(doc_ids)):
            posting = PostingTuple(doc_ids[posting_id], frequencies[posting_id])

            self.index.add(token, posting)
        
        self.assertListEqual(self.index.getDeltasOf(token), [1, 3, 3])
    
    def test_delta_steps_multiple_tokens(self):
        tokens_postings = {
            'test1':{
                'doc_ids': [1, 4, 7],
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
                self.index.add(token, new_posting)
        
        self.assertListEqual(self.index.getDeltasOf('test1'), [1, 3, 3])
        self.assertListEqual(self.index.getDeltasOf('test2'), [4, 96, 5])
    
    def test_empty_deltas(self):
        self.assertListEqual(self.index.getDeltasOf('test1'), [])
    
    def test_get_posting(self):
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
                self.index.add(token, new_posting)
        

        self.assertEqual(self.index.get_posting('test1', 7), PostingTuple(7, 7))
    
    def test_get_posting_with_checkpoint(self):
        token = 'test1'
        max_doc_id = InvertedList.CHECKPOINTS_EVERY + 2
        for doc_id in range(max_doc_id):
            frequency = doc_id
            posting = PostingTuple(doc_id, frequency)
            self.index.add(token, posting)
        
        goal_doc = max_doc_id-1
        posting_returned = self.index.get_posting(token, goal_doc)
        goal_posting = PostingTuple(goal_doc, goal_doc)
        self.assertEqual(posting_returned, goal_posting)
    
    def test_dont_get_posting(self):
        posting = PostingTuple(1,1)
        self.index.add('test1', posting)
        self.assertEqual(self.index.get_posting('test1', 2), None)
        self.assertEqual(self.index.get_posting('test2', 1), None)


if __name__ == '__main__':
    main()