from .resultset import ResultSet
from .gateway import init, connect

class VerdictContext:

    def __init__(self, url, usr, pwd):
        init()
        self.context = connect(url, usr, pwd)

    def sql(self, query):
        return ResultSet(self.context.sql(query))
