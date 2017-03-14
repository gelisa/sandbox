

class Custom_class(object):
    def __init__(self,filename):
        self.filename = filename

    def count_lines(self):
        count = 0
        with open(self.filename,'r') as f:
            for line in f:
                count += 1
        return count
