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
            self._index[token] = Postings()
        
        self._index[token].add(entry)
    
    def has_entry(self, token:str, doc_id:int) -> bool:
        if token not in self._index:
            return False
        
        return self._index[token].has_doc(doc_id)
    
    def getDeltasOf(self, token:str):
        return self._index[token].get_deltas()

class Postings():

    def __init__(self):
        self._last_id_total = 0
        self._postings = list()
    
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
    
    def add(self, posting:Posting):
        
        delta_id = self._get_delta_for_id(posting.doc_id)
        self._postings.append((delta_id, posting.frequency))

        self._last_id_total = self._last_id_total + delta_id
        
    def _get_delta_for_id(self, doc_id:int) -> int:
        return doc_id - self._last_id_total
    
    def has_doc(self, id:int) -> bool:
        for entry in self._postings:
            if entry[0] == id:
                return True
        
        return False
    
    def get_deltas(self):
        deltas = list()
        
        for posting in self._postings:
            deltas.append(posting[0])
        
        return deltas
    