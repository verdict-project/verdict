import os
from pyverdict import VerdictContext


def test_presto_factory_method():
    hostport = os.environ['VERDICTDB_TEST_PRESTO_HOST']
    host, port = hostport.split(':')
    port = int(port)
    catalog = os.environ['VERDICTDB_TEST_PRESTO_CATALOG']
    user = os.environ['VERDICTDB_TEST_PRESTO_USER']
    password = ''

    verdict = VerdictContext.newPrestoContext(host, catalog, user)
    result = verdict.sql('show schemas')
