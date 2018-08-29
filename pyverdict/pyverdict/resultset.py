class ResultSet:

    def __init__(self, resultset):
        self.set = resultset

    def fetch_one(self):
        self.set.next()
        return self.set.getInt(0)
