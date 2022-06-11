from collections import namedtuple

PostingTuple = namedtuple('PostingTuple', 'doc_id frequency')
        
class Index():
    def __init__(self):
        self._index = dict()
    
    @property
    def index(self):
        """
        The index structure
        """
        raise AttributeError("Index is not readable")

    @index.setter
    def index(self, new_index):
        raise AttributeError("index is not writable")

    def add(self, token:str, entry:PostingTuple):

        self._index.setdefault(token, InvertedList()).add(entry)
    
    def add_from_distribuition(self, distribuition:dict, doc_id:int):

        for token, frequency in distribuition.items():
            new_posting = PostingTuple(doc_id, frequency)
            self.add(token, new_posting)

    def has_entry(self, token:str, doc_id:int) -> bool:
        invertedList = self._get_inverted_list(token)

        if invertedList == None:
            return False
        else:
            return invertedList.has_doc(doc_id)
    
    def _get_inverted_list(self, token:str):
        return self._index.get(token, None)
    
    def get_posting(self, token:str, doc_id:int) -> PostingTuple:
        if token not in self._index:
            return None
        
        return self._index[token].get_posting(doc_id)
    
    def get_postings_of(self, token:str) -> list:
        return self._index[token].get_all_postings()
    
    def get_freq_of_doc_for_token(self, doc_id:int, token:str) -> int:
        inverted_list = self._get_inverted_list(token)

        if inverted_list == None:
            return 0
        else:
            return inverted_list.get_posting(doc_id).frequency
    
    def get_num_postings_for(self, token:str) ->int:
        inverted_list = self._get_inverted_list(token)

        if inverted_list == None:
            return 0
        else:
            return inverted_list.num_postings

class InvertedList():

    Posting = namedtuple('Posting', 'frequency')

    def __init__(self):
        self._postings = dict()
    
    @property
    def postings(self):
        """
        The postings collection
        """
        return self._postings
    
    @postings.setter
    def postings(self, new_postings):
        raise AttributeError("postings is not writable")
    
    @property
    def num_postings(self):
        """
        The number of postings in this inverted list
        """
        return len(self._postings)
    
    def add(self, new_posting:PostingTuple):
        
        self._postings.setdefault(new_posting.doc_id, {'frequency':0})['frequency']+=new_posting.frequency
    
    def get_posting(self, doc_id:int) -> PostingTuple:

        posting_for_doc = self._find(doc_id)

        if posting_for_doc == None:
            return None
        else:
            return PostingTuple(doc_id, posting_for_doc['frequency'])
    
    def _find(self, doc_id:int) -> tuple:
        return self._postings.get(doc_id, None)
    
    def get_all_postings(self) -> list:
        return [PostingTuple(doc_id, posting['frequency']) for doc_id, posting in self._postings.items()]
    
    def has_doc(self, doc_id:int) -> bool:
        return self._find(doc_id) != None
    
    def empty(self):
        return len(self._postings.keys()) == 0