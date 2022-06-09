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
            self._index[token] = []
        
        self._index[token].append(entry)
    
    def has_entry(self, token:str, doc_id:int) -> bool:
        if token not in self._index:
            return False
        
        for entry in self._index[token]:
            if entry.doc_id == doc_id:
                return True
        
        return False