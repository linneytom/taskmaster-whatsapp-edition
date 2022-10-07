import unittest
from unittest.mock import patch
from random_class import AClass

class ATestClass(unittest.TestCase):
    
    @patch.object(AClass, "input_is_not_str")
    def test_what_is_input_call_input_is_not_str(self, mock_input_is_not_str):
        strput = "123"
        intput = 123

        str_class = AClass(input_ = strput) # shoudnt call
        int_class = AClass(input_ = intput) # should call

        int_class.what_is_input()
        
        mock_input_is_not_str.assert_called()        

        str_class.what_is_input()

        mock_input_is_not_str.assert_called_once()




