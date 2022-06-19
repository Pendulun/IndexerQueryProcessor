from collections import namedtuple, Counter
import json
import sys
import pickle

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
    
    @property
    def size(self):
        """
        Returns a tuple of (numTokens, numTotalPostings)
        """
        return (self.get_num_tokens(), self._get_total_num_postings())
    
    def get_num_tokens(self):
        return len(self._index)
    
    def clear(self):
        self._index.clear()

    def get_stats(self):
        num_tokens = len(self._index)
        total_postings = self._get_total_num_postings()
        mean_postings = 0
        if num_tokens != 0:
            mean_postings = total_postings/num_tokens
        return num_tokens, mean_postings

    def mem_size(self):
        return self._get_size(self)
    
    def _get_size(self, obj, seen=None):
        """Recursively finds size of objects"""
        size = sys.getsizeof(obj)
        if seen is None:
            seen = set()
        obj_id = id(obj)
        if obj_id in seen:
            return 0
        # Important mark as seen *before* entering recursion to gracefully handle
        # self-referential objects
        seen.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self._get_size(v, seen) for v in obj.values()])
            size += sum([self._get_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self._get_size(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self._get_size(i, seen) for i in obj])
        return size

    def _get_total_num_postings(self):
        total_postings = 0
        for _, inverted_list in self._index.items():
            total_postings += inverted_list.size
        
        return total_postings

    def add(self, token:str, entry:PostingTuple):

        self._index.setdefault(token, InvertedList()).add(entry)
    
    def add_from_distribuition(self, distribuition:dict, doc_id:int):

        for token, frequency in distribuition.items():
            new_posting = PostingTuple(doc_id, frequency)
            self.add(token, new_posting)
    
    def update_from_list_of_tuples(self, list_of_tuples:list):
        for token, postings_list in list_of_tuples:
            for posting in postings_list:
                self.add(token, PostingTuple(posting[0], posting[1]))

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
    
    def get_entire_index(self):
        return {token:inverted_list.get_all_postings_as_dicts() 
                        for token, inverted_list in self._index.items()}
    
    def to_json(self):
        return json.dumps(self.get_entire_index(), indent=4, ensure_ascii=False)
    
    def get_postings_dist(self) -> Counter:
        """
        Returns the frequency distribuition for all tokens
        """
        dist = Counter()
        for _, inverted_list in self._index.items():
            for _, frequency in inverted_list.get_all_postings_as_tuples():
                dist[frequency] += 1
        
        return dist
    
    def get_postings_dist_as_json(self):
        return json.dumps(self.get_postings_dist(), indent=4, ensure_ascii=False)
    
    def get_index_as_tuples_gen(self):
        for token, inverted_list in sorted(self._index.items()):
             yield (token, inverted_list.get_all_postings_as_tuples())

    def save_to_pickle(self, file:str):
        #https://stackoverflow.com/questions/20716812/saving-and-loading-multiple-objects-in-pickle-file
        with open(file,'ab') as index_file:
            for token_postings_tuple in self.get_index_as_tuples_gen(): 
                pickle.dump(token_postings_tuple, index_file)
    
    def save_to_pickle_and_clear(self, file:str):
        self.save_to_pickle(file)
        self.clear()
    
    def load_index_from(self, file:str):
        with open(file,'rb') as index_file:
            while True:
                try:
                    self.update_from_list_of_tuples([pickle.load(index_file)])
                except EOFError:
                    break
    
    @classmethod
    def get_stats_from_file(cls, file:str):
        num_tokens = 0
        num_total_postings = 0
        with open(file,'rb') as index_file:
            while True:
                try:
                    token, postings_list = pickle.load(index_file)
                    num_tokens += 1
                    num_total_postings += len(postings_list)
                except EOFError:
                    break
        mean_postings_list = 0
        if num_tokens != 0:
            mean_postings_list = num_total_postings/num_tokens
            
        return num_tokens, mean_postings_list

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
    def size(self):
        """
        Num of postings in this inverted list
        """
        return len(self._postings)
    
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
    
    def get_all_postings_as_tuples(self) -> list:
        return [(doc_id, posting['frequency']) for doc_id, posting in self._postings.items()]
    
    def get_all_postings_as_dicts(self) -> list:
        return {doc_id:posting['frequency'] for doc_id, posting in self._postings.items()}
    
    def has_doc(self, doc_id:int) -> bool:
        return self._find(doc_id) != None
    
    def empty(self):
        return len(self._postings.keys()) == 0