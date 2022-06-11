from math import log

class GammaEncoder():
    """
    Based on: https://www.geeksforgeeks.org/elias-gamma-encoding-in-python/
    """

    def _unary(self, x):
        return (x-1)*'0'+'1'

    def _binary(self, num:int, num_bits = 1):
        s = '{0:0%db}' % num_bits
        return s.format(num)
    
    def encode(self, num:int) -> str:
        if(num == 0):
            return '0'

        biggest_two_power = 1 + int(log(num, 2))

        left_over = num - 2**(int(log(num, 2)))
        bits_for_left_over = int(log(num, 2))

        return self._unary(biggest_two_power) + self._binary(left_over, bits_for_left_over)
    
    def decode(self, gamma_encoded_num:str) -> int:
        
        biggest_two_power = 0
        for char in gamma_encoded_num:
            if char == '0':
                biggest_two_power+=1
            else:
                break
        
        left_over_bits = gamma_encoded_num[biggest_two_power+1:]
        left_over =  int(left_over_bits, base=2)

        result = (2**biggest_two_power) + left_over
        return result