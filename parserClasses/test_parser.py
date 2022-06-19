from myparser import TextParser
from unittest import TestCase, main

class TestTextParser(TestCase):
    
    def test_get_dist_letter_numbers(self):
        text = 'Uvas uva bananas uva Banana corações carrões'

        tokens_count = {
            'uva': 3,
            'banan': 2,
            'coraçã': 1,
            'carr': 1
        }

        self.assertDictEqual(TextParser.get_distribuition_of(text), tokens_count)

    def test_get_dist_empty_string(self):
        text = ""

        tokens_count = {}

        self.assertDictEqual(TextParser.get_distribuition_of(text), tokens_count)
    
    def test_get_dist_only_numbers(self):
        text = '1 1 49 0.55 3 3 3 2 49'

        tokens_count = {
            '1': 2,
            '2': 1,
            '3': 3,
            '0.55': 1,
            '49': 2
        }

        self.assertDictEqual(TextParser.get_distribuition_of(text), tokens_count)

    def test_get_dist_ignore_punctuation(self):
        text = 'A, B C C D B. D T T C B: A'

        tokens_count = {
            'b': 3,
            'c': 3,
            'd': 2,
            't': 2
        }

        self.assertDictEqual(TextParser.get_distribuition_of(text), tokens_count)
    
    def test_get_dist_ignore_stop_words(self):
        text = 'O pêlo no pé do Pedro é preto'

        unwanted = ['o', 'no','do','é']
        distribuition = TextParser.get_distribuition_of(text)
        self.assertTrue(all(map(lambda x: x not in unwanted, distribuition.keys())))
    
    def test_get_dist_empty_str(self):
        text=""
        self.assertDictEqual(TextParser.get_distribuition_of(text), {})
    
    def test_get_dist_text_with_endline(self):
        text='O pêlo no pé do Pedro é preto\n'
        self.assertTrue(len(TextParser.get_distribuition_of(text).keys()) > 0)
    
    def test_get_dist_text_with_spaces_at_end_beginning(self):
        text='  O pêlo no pé do Pedro é preto   '
        self.assertTrue(len(TextParser.get_distribuition_of(text).keys()) > 0)
    
    def test_get_dist_text_with_spaces_and_endline(self):
        text='  O pêlo no pé do Pedro é preto   \n'
        self.assertTrue(len(TextParser.get_distribuition_of(text).keys()) > 0)
    
    def test_get_dist_text_with_endline_in_middle(self):
        text='  O pêlo no pé do\n Pedro é preto   \n'
        self.assertTrue(len(TextParser.get_distribuition_of(text).keys()) > 0)
    
    def test_pre_proccess_type_error(self):
        self.assertRaises(TypeError, TextParser.pre_proccess, ['teste1','teste2'])
    
    def test_stem_type_error(self):
        self.assertRaises(TypeError, TextParser.stem, 'teste testando')

    def test_split_on_upper(self):
        text = "TestandoAqui o splitSeDeu certo"
        self.assertEqual(TextParser.split_on_upper(text), "Testando Aqui o split Se Deu certo")
    
    def test_split_on_upper_no_upper(self):
        text = "Testandoqui o splitsedeu certo"
        self.assertEqual(TextParser.split_on_upper(text), text)
    
    def test_split_on_upper_empty_str(self):
        text = ""
        self.assertEqual(TextParser.split_on_upper(text), text)

if __name__ == '__main__':
    main()