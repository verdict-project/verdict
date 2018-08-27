from resultset import ResultSet]
from gateway import Gateway

global gateway

class VerdictContext:
    def __init__(self, url, usr, pwd):
        if gateway is None:
            gateway = Gateway()
        self.context = gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(url, usr, pwd)

    def sql(self, query):
        return ResultSet(self.context.sql(query))
