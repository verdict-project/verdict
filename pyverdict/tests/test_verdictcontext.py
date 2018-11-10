import os
from pyverdict import VerdictContext


def test_presto_factory_method():
    hostport = os.environ['VERDICTDB_TEST_PRESTO_HOST']
    host, port = hostport.split(':')
    port = int(port)
    catalog = os.environ['VERDICTDB_TEST_PRESTO_CATALOG']
    user = os.environ['VERDICTDB_TEST_PRESTO_USER']
    password = ''

    verdict = VerdictContext.newPrestoContext(host, catalog, user, port=port)
    result = verdict.sql('show schemas')


def test_mysql_factory_method():
    host = 'localhost'
    port = 3306
    user = 'root'
    password = ''

    verdict = VerdictContext.newMySqlContext(host, user, password)
    result = verdict.sql('show schemas')
