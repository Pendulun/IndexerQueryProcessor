from typing import Text
from nltk import FreqDist, corpus, stem
from nltk.tokenize import word_tokenize
import string

class TextParser():

    COMPLETE_FILTER = set(corpus.stopwords.words('portuguese')).union(set(char for char in string.punctuation))
    PORTUGUESE_STEMMER = stem.RSLPStemmer()

    @classmethod
    def get_distribuition_of(self, text:str) -> dict:
        
        tokens = [word for word in TextParser.pre_proccess(text) if word not in TextParser.COMPLETE_FILTER]

        tokens = TextParser.stem(tokens) 
        
        token_frequency = FreqDist(tokens)

        return dict(token_frequency.items())
    
    @classmethod
    def pre_proccess(self, text:str) -> list:
        if type(text) != str:
            raise TypeError("text should be a str")
        return [word.lower() for word in word_tokenize(text.rstrip('\n').strip())]
    
    @classmethod
    def stem(self, text_list:list) -> list:
        if type(text_list) != list:
            raise TypeError("text_list should be a list of str")
        
        return [TextParser.PORTUGUESE_STEMMER.stem(token) for token in text_list]