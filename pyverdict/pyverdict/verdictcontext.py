import pyverdict

class VerdictContext:
    def __init__(self, gateway, host, port, database, user, pwd):
        config = 'jdbc:mysql://{}:{}/{}'.format(host, port, database)
        print(config)
        self.context = gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(config, 'root', '')

    def sql(self, query):
        return pyverdict.ResultSet(self.context.sql(query))
