class ResultSet:
    def __init__(self, resultset):
        self.set = resultset
    def parse(self):
        while (self.set.next()):
            print(self.set.getInt(0))
