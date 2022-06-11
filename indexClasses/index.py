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
    
    def getDeltasOf(self, token:str):
        invertedList = self._get_inverted_list(token)

        if invertedList == None:
            return list()
        else:
            return invertedList.get_deltas()
    
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

    CHECKPOINTS_EVERY = 1000

    def __init__(self):
        self._last_id_total = 0
        self._postings = list()
        self._checkpoints = dict()
        self._num_postings = 0
    
    @property
    def last_id_total(self):
        """
        The last/max doc id present
        """
        return self._last_id_total
    
    @last_id_total.setter
    def last_id_total(self, new_last_id):
        raise AttributeError("last_id_total is not writable")
    
    @property
    def postings(self):
        """
        The postings collections
        """
        return self._postings
    
    @postings.setter
    def postings(self, new_postings):
        raise AttributeError("postings is not writable")
    
    @property
    def checkpoints(self):
        """
        The checkpoints for this inverted list as it implements delta stepping
        """
        return self._checkpoints
    
    @checkpoints.setter
    def checkpoints(self, new_checkpoints):
        raise AttributeError("checkpoints is not writable")
    
    @property
    def num_postings(self):
        """
        The number of postings in this inverted list
        """
        return self._num_postings
    
    @num_postings.setter
    def num_postings(self, new_num_postings):
        raise AttributeError("num_postings is not writable")
    
    class Posting():
    
        def __init__(self, delta_gap:int, frequency:int):
            self._delta_gap = delta_gap
            self._frequency = frequency
        
        @property
        def delta_gap(self):
            """
            The document delta gap
            """
            return self._delta_gap
        
        @delta_gap.setter
        def delta_gap(self, new_delta_gap):
            raise AttributeError("delta_gap is not writable")
        
        @property
        def frequency(self):
            """
            The token frequency at this doc
            """
            return self._frequency
        
        @frequency.setter
        def frequency(self, new_freq):
            raise AttributeError("frequency is not writable")
        
        def add_frequency(self, freq_count):
            self._frequency += freq_count
        
        def __eq__(self, __o: object) -> bool:
            if not isinstance(__o, InvertedList.Posting):
                # don't attempt to compare against unrelated types
                return NotImplemented
            
            equal = self.doc_id == __o.doc_id
            equal = equal and self.frequency == __o.frequency
            return equal
        
        def __hash__(self) -> int:
            my_hash = hash(self._doc_id)
            my_hash += hash(self._frequency)
            return my_hash
        
        def __str__(self) -> str:
            return "Posting(doc_id="+str(self._doc_id)+", frequency="+str(self._frequency)+")"
    
    def add(self, new_posting:PostingTuple):
        
        existing_posting = self._find(new_posting.doc_id, True)
        if existing_posting == None:
            new_posting = self._insert_new_posting(new_posting)
        else:
            existing_posting.add_frequency(new_posting.frequency)

    def _insert_new_posting(self, new_posting:PostingTuple):
        delta_id = self._get_delta_for_id(new_posting.doc_id)

        new_posting = InvertedList.Posting(delta_id, new_posting.frequency)
            
        self._postings.append(new_posting)

        self._last_id_total += delta_id
        self._num_postings += 1

        if self._num_postings % InvertedList.CHECKPOINTS_EVERY == 0:
            self._checkpoints[self._last_id_total] = self._num_postings - 1
        return new_posting
           
    def _get_delta_for_id(self, doc_id:int) -> int:
        return doc_id - self._last_id_total
    
    def get_posting(self, doc_id:int) -> PostingTuple:

        posting = self._find(doc_id, False)

        if posting == None:
            return None
        else:
            return PostingTuple(posting.delta_gap, posting.frequency)
    
    def _find(self, doc_id:int, delta_gapped:bool) -> Posting:
        if self.empty():
            return None
        
        acc_doc_id, checkpoint_pos = self._get_checkpoint_for(doc_id)
        
        for posting_count, posting in enumerate(self._postings[checkpoint_pos:]):
            if acc_doc_id == None:
                acc_doc_id = posting.delta_gap
            elif posting_count != 0:
                acc_doc_id += posting.delta_gap

            if acc_doc_id == doc_id:
                if delta_gapped:
                    return posting
                else:
                    return InvertedList.Posting(acc_doc_id, posting.frequency)
            elif acc_doc_id > doc_id:
                return None

        return None
    
    def _get_checkpoint_for(self, doc_id:int) -> tuple:
        
        checkpoint_pos = 0
        checkpoint_id = None
        for id_checkpoint, pos in self._checkpoints.items():
            if id_checkpoint > doc_id:
                return checkpoint_id, checkpoint_pos
            
            checkpoint_id = id_checkpoint
            checkpoint_pos = pos

        return checkpoint_id, checkpoint_pos
    
    def get_all_postings(self) -> list:
        return [posting for posting in self._postings]
    
    def has_doc(self, doc_id:int) -> bool:
        return self._find(doc_id, True) != None
    
    def get_deltas(self) -> list:
        return [posting.delta_gap for posting in self._postings]
    
    def empty(self):
        return self._num_postings == 0