class Posting():
    
    def __init__(self, doc_id:int, frequency:int):
        self._doc_id = doc_id
        self._frequency = frequency
    
    @property
    def doc_id(self):
        """
        The document Id
        """
        return self._doc_id
    
    @doc_id.setter
    def doc_id(self, new_doc_id):
        raise AttributeError("doc_id is not writable")
    
    @property
    def frequency(self):
        """
        The token frequency at this doc
        """
        return self._frequency
    
    @frequency.setter
    def frequency(self, new_freq):
        raise AttributeError("frequency is not writable")
    
    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Posting):
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

    def add(self, token:str, entry:Posting):
        if token not in self._index:
            self._index[token] = InvertedList()
        
        self._index[token].add(entry)
    
    def has_entry(self, token:str, doc_id:int) -> bool:
        if token not in self._index:
            return False
        
        return self._index[token].has_doc(doc_id)
    
    def getDeltasOf(self, token:str):
        if token in self._index: 
            return self._index[token].get_deltas()
        else:
            return list()
    
    def get_posting(self, token:str, doc_id:int) -> Posting:
        if token not in self._index:
            return None
        
        return self._index[token].get(doc_id)
    
    def get_postings_of(self, token:str) -> list:
        return self._index[token].get_all_postings()

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
    
    def add(self, posting:Posting):
        
        delta_id = self._get_delta_for_id(posting.doc_id)
        self._postings.append((delta_id, posting.frequency))

        self._last_id_total += delta_id
        self._num_postings += 1

        if self._num_postings % InvertedList.CHECKPOINTS_EVERY == 0:
            self._checkpoints[self._last_id_total] = self._num_postings - 1
        
    def _get_delta_for_id(self, doc_id:int) -> int:
        return doc_id - self._last_id_total
    
    def has_doc(self, doc_id:int) -> bool:

        acc_doc_id, checkpoint_pos = self._get_checkpoint_for(doc_id)
        
        for posting_count, posting in enumerate(self._postings[checkpoint_pos:]):
            if acc_doc_id == None:
                acc_doc_id = posting[0]
            elif posting_count != 0:
                acc_doc_id += posting[0]
            
            if acc_doc_id == doc_id:
                return True
        
        return False
    
    def get_deltas(self):
        deltas = list()
        
        for posting in self._postings:
            deltas.append(posting[0])
        
        return deltas
    
    def get(self, doc_id:int) -> Posting:

        if self._num_postings == 0:
            return None
        
        acc_doc_id, checkpoint_pos = self._get_checkpoint_for(doc_id)
        
        for posting_count, posting in enumerate(self._postings[checkpoint_pos:]):
            if acc_doc_id == None:
                acc_doc_id = posting[0]
            elif posting_count != 0:
                acc_doc_id += posting[0]

            if acc_doc_id == doc_id:
                return Posting(acc_doc_id, posting[1])
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
        posting_list = list()
        for posting in self._postings:
            posting_list.append(posting)
        
        return posting_list