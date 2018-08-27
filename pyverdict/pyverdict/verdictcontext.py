from .resultset import ResultSet
from .gateway import Gateway


class VerdictContext:

    def __init__(self, url, usr, pwd):
        global single_gateway

        if single_gateway is None:
            print('initializing new jvm')
            single_gateway = Gateway()
        self.context = single_gateway.connect(url, usr, pwd)

    def sql(self, query):
        return ResultSet(self.context.sql(query))
