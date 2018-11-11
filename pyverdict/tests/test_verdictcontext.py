import os
import pyverdict
from pyverdict import VerdictContext


def test_presto_factory_method():
    hostport = os.environ['VERDICTDB_TEST_PRESTO_HOST']
    host, port = hostport.split(':')
    port = int(port)
    catalog = os.environ['VERDICTDB_TEST_PRESTO_CATALOG']
    user = os.environ['VERDICTDB_TEST_PRESTO_USER']
    password = ''

    verdict = VerdictContext.new_presto_context(host, catalog, user, port=port)
    print(verdict.sql('show schemas'))

def test_presto_init_method():
    hostport = os.environ['VERDICTDB_TEST_PRESTO_HOST']
    host, port = hostport.split(':')
    port = int(port)
    catalog = os.environ['VERDICTDB_TEST_PRESTO_CATALOG']
    user = os.environ['VERDICTDB_TEST_PRESTO_USER']
    password = ''

    verdict = pyverdict.presto_context(host, catalog, user, port=port)
    print(verdict.sql('show schemas'))

def test_mysql_factory_method():
    host = 'localhost'
    port = 3306
    user = 'root'
    password = ''

    verdict = VerdictContext.new_mysql_context(host, user, password)
    print(verdict.sql('show schemas'))

def test_mysql_init_method():
    host = 'localhost'
    port = 3306
    user = 'root'
    password = ''

    verdict = pyverdict.mysql_context(host, user, password)
    print(verdict.sql('show schemas'))


