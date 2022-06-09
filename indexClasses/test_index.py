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

if __name__ == '__main__':
    main()