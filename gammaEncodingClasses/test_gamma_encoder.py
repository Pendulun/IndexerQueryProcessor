from unittest import TestCase, main
from gamma_encoder import *

class TestGammaEncoder(TestCase):
    def setUp(self):
        self.encoder = GammaEncoder()

    def test_encode(self):
        num = 5
        self.assertEqual(self.encoder.encode(num), '00101')
        num = 10
        self.assertEqual(self.encoder.encode(num), '0001010')
        num = 12
        self.assertEqual(self.encoder.encode(num), '0001100')
        num= 100
        self.assertEqual(self.encoder.encode(num), '0000001100100')
    
    def test_raise_on_negative_number(self):
        num = -3
        self.assertRaises(ValueError, self.encoder.encode, num)
    
    def test_raise_on_invalid_type(self):
        num = '3'
        self.assertRaises(TypeError, self.encoder.encode, num)
    
    def test_decoder(self):
        ten_gamma_encoded = '0001010'
        twelve_gamma_encoded = '0001100'
        one_hundred_gamma_encoded = '0000001100100'
        self.assertEqual(self.encoder.decode(ten_gamma_encoded), 10)
        self.assertEqual(self.encoder.decode(twelve_gamma_encoded), 12)  
        self.assertEqual(self.encoder.decode(one_hundred_gamma_encoded), 100)
    
    def test_encode_decode(self):
        num = 100
        gamma_encoding = self.encoder.encode(num)
        self.assertEqual(self.encoder.decode(gamma_encoding), num)
    
    def test_raise_decode_invalid_gamma(self):
        invalid_gamma_encoding = '0001'
        self.assertRaises(Exception, self.encoder.decode, invalid_gamma_encoding)

    

if __name__ == '__main__':
    main()