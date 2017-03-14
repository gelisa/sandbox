"""
this class takes a filename and creates a very simple object
with the only attribute filename
"""

class Custom_class(object):
    def __init__(self,filename):
        self.filename = filename

    def count_lines(self):
        """
        let us assume we want to use class method to create a class attribute
        here we create self.count, which is line count for the file located at self.filename
        :return: self
        it has to return self if we want access self.count for rdd's in Spark are immutable
        """
        count = 0
        with open(self.filename,'r') as f:
            for line in f:
                count += 1
        self.count = count
        return self

    def get_stats(self):
        """
        Let us imagine we want to get some stats for our object
        We create a dictionary that for this file stores self.count/2 and self.count/4
        :return: list of key value pairs: this is how Spark works with map (dictionary) objects
        """
        d = {'half_count': self.count/2, 'quarter_count': self.count/4}
        return d.items()
