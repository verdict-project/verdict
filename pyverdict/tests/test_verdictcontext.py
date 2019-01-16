'''
    Copyright 2018 University of Michigan

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
'''
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

    verdict.close()

def test_presto_init_method():
    hostport = os.environ['VERDICTDB_TEST_PRESTO_HOST']
    host, port = hostport.split(':')
    port = int(port)
    catalog = os.environ['VERDICTDB_TEST_PRESTO_CATALOG']
    user = os.environ['VERDICTDB_TEST_PRESTO_USER']
    password = ''

    verdict = pyverdict.presto_context(host, catalog, user, port=port)
    print(verdict.sql('show schemas'))

    verdict.close()

def test_mysql_factory_method():
    host = 'localhost'
    port = 3306
    user = 'root'
    password = ''

    verdict = VerdictContext.new_mysql_context(host, user, password)
    print(verdict.sql('show schemas'))

    verdict.close()

def test_mysql_init_method():
    host = 'localhost'
    port = 3306
    user = 'root'
    password = ''

    verdict = pyverdict.mysql_context(host, user, password)
    print(verdict.sql('show schemas'))

    verdict.close()

def test_postgres_factory_method():
    host = 'localhost'
    port = 5432
    user = 'postgres'
    dbname = 'postgres'
    password = None

    verdict = VerdictContext.new_postgres_context(
        dbname,
        user,
        password,
        host,
        port,
    )

    print(verdict.sql('show schemas'))

    verdict.close()

def test_postgres_init_method():
    host = 'localhost'
    port = 5432
    user = 'postgres'
    dbname = 'postgres'
    password = None

    verdict = pyverdict.postgres_context(
        host,
        port,
        dbname,
        user,
        password,
    )

    print(verdict.sql('show schemas'))

    verdict.close()

def test_redshift_factory_method():
    host, port = os.environ['VERDICTDB_TEST_REDSHIFT_ENDPOINT'].split(':')
    port = int(port)

    user = os.environ['VERDICTDB_TEST_REDSHIFT_USER']
    password = os.environ['VERDICTDB_TEST_REDSHIFT_PASSWORD']
    dbname = 'dev'

    verdict = VerdictContext.new_redshift_context(
        host,
        port,
        dbname,
        user,
        password,
    )

    print(verdict.sql('show schemas'))

    verdict.close()

def test_redshift_init_method():
    host, port = os.environ['VERDICTDB_TEST_REDSHIFT_ENDPOINT'].split(':')
    port = int(port)

    user = os.environ['VERDICTDB_TEST_REDSHIFT_USER']
    password = os.environ['VERDICTDB_TEST_REDSHIFT_PASSWORD']
    dbname = 'dev'

    verdict = pyverdict.redshift_context(
        host,
        port,
        dbname,
        user,
        password,
    )

    print(verdict.sql('show schemas'))

    verdict.close()

def test_impala_factory_method():
    host, port = os.environ['VERDICTDB_TEST_IMPALA_HOST'].split(':')
    port = int(port)

    verdict = VerdictContext.new_impala_context(
        host,
        port,
        verdictdbmetaschema='test_meta_schema_impala',
    )

    print(verdict.sql('show schemas'))

    verdict.close()

