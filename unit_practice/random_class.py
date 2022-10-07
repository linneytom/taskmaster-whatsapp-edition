class AClass():
    def __init__(self, input_):
        self.input_ = input_

    def what_is_input(self):
        
        if not self.input_is_str():
            self.input_is_not_str()

    def input_is_str(self):
        return type(self.input_) == str
    
    def input_is_not_str(self):
        print("its not a string")