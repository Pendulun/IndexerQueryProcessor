from nltk import FreqDist, corpus, stem, TextCat
from nltk.tokenize import word_tokenize
import string

class TextParser():

    COMPLETE_FILTER = set(corpus.stopwords.words('portuguese')).union(set(char for char in string.punctuation))
    PORTUGUESE_STEMMER = stem.RSLPStemmer()
    MAX_TAM_WORD=23
    TEXT_CLASSIFIER = TextCat()
    SPLIT_ON = 'A B C D E F G I J L M N O P R S T U V'.split()
    ACCEPTED_LANGUAGES_DETECTED = ['por', 'glg']

    @classmethod
    def get_distribuition_of(cls, text:str) -> dict:
        
        tokens = [word for word in TextParser.pre_proccess(text) if word not in TextParser.COMPLETE_FILTER]

        tokens = TextParser.stem(tokens) 
        
        token_frequency = FreqDist(tokens)

        return dict(token_frequency.items())
    
    @classmethod
    def pre_proccess(cls, text:str) -> list:

        if type(text) != str:
            raise TypeError("text should be a str")

        text = TextParser.split_on_upper(text)
        text = text.rstrip('\n').strip()
        return [word.lower() for word in word_tokenize(text) if len(word) <= TextParser.MAX_TAM_WORD]

    @classmethod
    def stem(cls, text_list:list) -> list:
        if type(text_list) != list:
            raise TypeError("text_list should be a list of str")
        
        return [TextParser.PORTUGUESE_STEMMER.stem(token) for token in text_list]
    
    @classmethod
    def split_on_upper(cls, text:str) -> str:
        
        for letter in TextParser.SPLIT_ON:
            text = text.replace(letter, " "+letter)
        
        return text.replace("  ", " ").lstrip()
    
    @classmethod
    def is_portuguese(cls, text:str) -> bool:
        #return TextParser.language_of(text[:50]) in TextParser.ACCEPTED_LANGUAGES_DETECTED
        return True
    
    @classmethod
    def language_of(cls, text:str) -> str:
        return TextParser.TEXT_CLASSIFIER.guess_language(text)