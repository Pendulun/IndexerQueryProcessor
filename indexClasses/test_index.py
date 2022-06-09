from unittest import TestCase, main
from index import Index, Posting

class TestIndex(TestCase):

    def setUp(self):
        self.index = Index()

    def test_can_add_to_index(self):
        token = 'test'
        doc_id = 1
        frequency = 7
        posting = Posting(doc_id, frequency)

        self.index.add(token, posting)

        self.assertTrue(self.index.has_entry(token, posting.doc_id))
    
    def test_can_add_multiple_to_index(self):
        tokens = ['test1', 'test2', 'test3']
        doc_ids = [1, 1, 3]
        frequencies = [7, 4, 9]

        for posting_id in range(len(tokens)):
            posting = Posting(doc_ids[posting_id], frequencies[posting_id])

            self.index.add(tokens[posting_id], posting)
        

        for posting_id in range(len(tokens)):
            self.assertTrue(self.index.has_entry(tokens[posting_id], doc_ids[posting_id]))
    
    def test_doesnt_have_posting_without_token(self):
        self.assertFalse(self.index.has_entry('test', 1))
    
    def test_doesnt_have_posting_with_token(self):
        token = 'test'
        doc_id = 1
        frequency = 3

        posting = Posting(doc_id, frequency)
        self.index.add(token, posting)

        self.assertFalse(self.index.has_entry('test', 2))
    
    def test_delta_steps(self):
        token = 'test1'
        doc_ids = [1, 4, 7]
        frequencies = [3, 5, 7]

        for posting_id in range(len(doc_ids)):
            posting = Posting(doc_ids[posting_id], frequencies[posting_id])

            self.index.add(token, posting)
        
        self.assertListEqual(self.index.getDeltasOf(token), [1, 3, 3])


if __name__ == '__main__':
    main()