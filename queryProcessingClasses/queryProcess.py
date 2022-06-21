from parserClasses.myparser import TextParser
from math import log
import pathlib
import pickle

class QueryProcessor():
    
    TOP_N_DOCS = 10
    SCORING_METHODS = ('TFIDF', 'BM25')

    def __init__(self):
        self._index_file_path = None
        self._scoring_method = 'TFIDF'
        self._number_of_documents_in_index = 960000
        self._doc_id_to_url_file_path = pathlib.Path('id_to_doc/id_to_doc_map.pickle')
    
    @property
    def index_file_path(self):
        """
        The index file from where to query
        """
        return self._index_file_path
    
    @index_file_path.setter
    def index_file_path(self, new_index_file_path):
        if type(new_index_file_path) == str:
            self._index_file_path = pathlib.Path(new_index_file_path)
        elif isinstance(new_index_file_path, pathlib.Path):
            self._index_file_path = new_index_file_path
        else:
            raise TypeError("new_index_file_path should be a str or a pathlib.Path!")
    
    @property
    def scoring_method(self):
        """
        The documents scoring method. Should be one of: {TFIDF, BM25}
        """
        return self._scoring_method
    
    @scoring_method.setter
    def scoring_method(self, new_scoring_method:str):
        if new_scoring_method in QueryProcessor.SCORING_METHODS:
            self._scoring_method = new_scoring_method
        else:
            raise ValueError(f"new_scoring_method should be one of {QueryProcessor.SCORING_METHODS}")
    
    @property
    def num_docs_in_index(self):
        """
        The number of documents in the index.
        """
        return self._number_of_documents_in_index
    
    @num_docs_in_index.setter
    def num_docs_in_index(self, new_num:int):
        if type(new_num) == int:
            self._number_of_documents_in_index = new_num
        else:
            raise TypeError("new_num should be an int!")
    
    @property
    def doc_id_to_url_file_path(self):
        """
        The doc id to url mapping file
        """
        return self._doc_id_to_url_file_path
    
    @doc_id_to_url_file_path.setter
    def doc_id_to_url_file_path(self, new_doc_to_url_map):
        if type(new_doc_to_url_map) == str:
            self._doc_id_to_url_file_path = pathlib.Path(new_doc_to_url_map)
        elif isinstance(new_doc_to_url_map, pathlib.Path):
            self._doc_id_to_url_file_path = new_doc_to_url_map
        else:
            raise TypeError("new_doc_to_url_map should be a str or pathlib.Path")

    def process_queries(self, queries_list:list):
        queries_results = []
        for query in queries_list:
            queries_results.append(self.process_query(query))
        
        return queries_results

    def process_query(self, query:str) -> list:
        return self._query(query)

    def _query(self, query:str) -> list:

        ordered_query_tokens = sorted(list(TextParser.pre_proccess(query)))

        inverted_lists_of_interest = self._find_inverted_lists_of(ordered_query_tokens)

        top_n_scored_docs = self._score_docs_for_query(inverted_lists_of_interest, ordered_query_tokens)
        inverted_lists_of_interest = None

        return top_n_scored_docs

    def _find_inverted_lists_of(self, ordered_query_tokens:list) -> list:
        
        inverted_lists_of_interest = list()

        with open(self._index_file_path, 'rb') as index_file:
            searched_token_idx = 0
            curr_token_searched = ordered_query_tokens[searched_token_idx]
            total_query_tokens = len(ordered_query_tokens)
            
            while True:
                try:
                    curr_index_line = pickle.load(index_file)
                except EOFError:
                    break

                curr_line_token = curr_index_line[0]

                if curr_line_token > curr_token_searched:
                    searched_token_idx += 1
                    if searched_token_idx < total_query_tokens:
                        curr_token_searched = ordered_query_tokens[searched_token_idx]
                    else:
                        break

                if curr_line_token == curr_token_searched:
                    curr_token_postings = curr_index_line[1]
                    inverted_lists_of_interest.append(curr_token_postings)
                    searched_token_idx += 1
                    if searched_token_idx < total_query_tokens:
                        curr_token_searched = ordered_query_tokens[searched_token_idx]
                    else:
                        break
        
        return inverted_lists_of_interest
        
    def _score_docs_for_query(self, inverted_lists_of_interest:list, ordered_query_tokens:list) -> list:
        
        scoring_function = self._get_scoring_function(self._scoring_method)

        top_n_docs = self.DAAT_score(inverted_lists_of_interest, scoring_function)

        top_n_docs_converted = self.convert_ranking_doc_ids_to_urls(top_n_docs)

        return top_n_docs_converted

    def convert_ranking_doc_ids_to_urls(self, top_n_docs:list):
        
        converted_ranking = list()
        urls_mapping = []
        with open(self._doc_id_to_url_file_path, 'rb') as doc_id_to_urls_file:
            urls_mapping = list(pickle.load(doc_id_to_urls_file))
        
        for doc_score, doc_id in top_n_docs:
            doc_url = self._find_url_of_doc_id(doc_id, urls_mapping)
            converted_ranking.append((doc_score, doc_url))
        
        return converted_ranking
    
    def _find_url_of_doc_id(self, doc_id:int, urls_mapping:list) -> str :

        doc_url = ""

        for mapping in urls_mapping:
            mapped_doc_id, mapped_doc_url = mapping

            if mapped_doc_id == doc_id:
                doc_url = mapped_doc_url
                break

        return doc_url
    
    def DAAT_score(self, inverted_lists_of_interest:list, scoring_function):

        top_scored_docs = list()

        inverted_lists_pos = [0 for _ in range(len(inverted_lists_of_interest))]
        inverted_lists_lens = [len(inverted_list) for inverted_list in inverted_lists_of_interest]

        inv_lists_ended = set()
        curr_doc_id_in_inv_lists = dict()
        curr_inv_lists_associated_with_docs = set()

        while True:

            self._get_curr_doc_ids_from_inv_lists(inverted_lists_pos, inverted_lists_lens, curr_inv_lists_associated_with_docs, 
                                                    inverted_lists_of_interest, curr_doc_id_in_inv_lists, inv_lists_ended)
        
            if len(inv_lists_ended) == len(inverted_lists_of_interest):
                break

            smallest_doc_id, associated_inv_lists_idx = sorted(curr_doc_id_in_inv_lists.items())[0]

            final_doc_score = self._get_score_for_doc(inverted_lists_of_interest, scoring_function, inverted_lists_pos,
                                                         associated_inv_lists_idx)
        
            top_scored_docs = self._add_doc_to_ranking(top_scored_docs, smallest_doc_id, final_doc_score)

            for associated_inv_list_idx in associated_inv_lists_idx:
                curr_inv_lists_associated_with_docs.remove(associated_inv_list_idx)
            
            del curr_doc_id_in_inv_lists[smallest_doc_id]
        
        return top_scored_docs

    def _add_doc_to_ranking(self, top_scored_docs:list, smallest_doc_id, final_doc_score):

        top_scored_docs.append((final_doc_score, smallest_doc_id))
        top_scored_docs.sort()
        if len(top_scored_docs) > QueryProcessor.TOP_N_DOCS:
            top_scored_docs.pop(0)

        return top_scored_docs

    def _get_score_for_doc(self, inverted_lists_of_interest, scoring_function, inverted_lists_pos, associated_inv_lists_idx):

        doc_inv_lists = list()
        doc_frequencies = list()
        for inv_list_idx in associated_inv_lists_idx:
            curr_inv_list = inverted_lists_of_interest[inv_list_idx]
            doc_inv_lists.append(curr_inv_list)
            curr_inv_list_pos = inverted_lists_pos[inv_list_idx] - 1
            posting = curr_inv_list[curr_inv_list_pos]
            frequency = posting[1]
            doc_frequencies.append(frequency)

        inv_list_and_doc_freqs = zip(doc_inv_lists, doc_frequencies)

        doc_scores = [scoring_function(inv_list, doc_freq) for (inv_list, doc_freq) in inv_list_and_doc_freqs]
        final_doc_score = 0
        if len(doc_scores) > 0:
            final_doc_score = sum(doc_scores)
        return final_doc_score

    def _get_curr_doc_ids_from_inv_lists(self, inverted_lists_pos, inverted_lists_lens, curr_inv_lists_associated_with_docs, 
                                            inverted_lists_of_interest, curr_doc_id_in_inv_lists, inv_lists_ended):

        for inv_list_idx, inv_list_curr_pos in enumerate(inverted_lists_pos):
            if inv_list_curr_pos < inverted_lists_lens[inv_list_idx]:
                if inv_list_idx not in curr_inv_lists_associated_with_docs:
                    curr_inv_list_doc_id = inverted_lists_of_interest[inv_list_idx][inv_list_curr_pos][0]
                    inverted_lists_pos[inv_list_idx] += 1
                    curr_doc_id_in_inv_lists.setdefault(curr_inv_list_doc_id, []).append(inv_list_idx)
                    curr_inv_lists_associated_with_docs.add(inv_list_idx)
            else:
                inv_lists_ended.add(inv_list_idx)
        
    def _get_scoring_function(self, scoring_method:str):
        if self._scoring_method == 'TFIDF':
            return self._tfidf
        elif self._scoring_method == 'BM25':
            return self._bm25

    def _tfidf(self, inv_list:list, doc_freq:int):
        return doc_freq*log(self._number_of_documents_in_index/len(inv_list))

    def _bm25(self, inv_list:list, doc_freq:int):
        return self._tfidf(inv_list, doc_freq)