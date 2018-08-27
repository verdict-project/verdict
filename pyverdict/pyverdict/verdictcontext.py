from .resultset import ResultSet
import .gateway

class VerdictContext:

    def __init__(self, url, usr, pwd):
        single_gateway = gateway.init()
        self.context = single_gateway.connect(url, usr, pwd)

    def sql(self, query):
        return ResultSet(self.context.sql(query))
