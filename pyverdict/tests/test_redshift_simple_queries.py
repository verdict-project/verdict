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
import pyverdict
import psycopg2
import uuid
import os

REDSHIFT_DBNAME = 'dev'
TEST_SCHEMA_NAME = 'pyverdict_redshift_simple_query_test_schema' + str(uuid.uuid4())[:3]
TEST_TABLE_NAME = 'test_table'
TEST_SCRAMBLE_NAME = 'test_scramble'

redshift_conn = None
verdict_conn = None

def setup_module():
    global redshift_conn
    global verdict_conn

    host, port = os.environ['VERDICTDB_TEST_REDSHIFT_ENDPOINT'].split(':')
    port = int(port)

    user = os.environ['VERDICTDB_TEST_REDSHIFT_USER']
    password = os.environ['VERDICTDB_TEST_REDSHIFT_PASSWORD']

    # create table and populate data
    redshift_conn = psycopg2_connect(host, port, user, password)
    redshift_conn.set_session(autocommit=True)

    cur = redshift_conn.cursor()

    cur.execute('DROP SCHEMA IF EXISTS "%s" CASCADE' % TEST_SCHEMA_NAME)
    cur.execute('CREATE SCHEMA IF NOT EXISTS "%s"' % TEST_SCHEMA_NAME)
    cur.execute(get_create_table_str())

    cur.execute(get_insert_into_table_str())
    cur.close()

    verdict_conn = verdict_connect(host, port, user, password)
    verdict_conn.sql(get_create_scramble_str())


def psycopg2_connect(host, port, user, password):
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=REDSHIFT_DBNAME,
    )


def verdict_connect(host, port, username, password):
    return pyverdict.VerdictContext.new_redshift_context(
        host,
        port,
        REDSHIFT_DBNAME,
        username,
        password,
    )


def get_create_table_str():
    return '''
        CREATE TABLE IF NOT EXISTS %s.%s (
            intCol            integer
        )
    ''' % (
        TEST_SCHEMA_NAME,
        TEST_TABLE_NAME,
    )


def get_insert_into_table_str():
    return '''
        INSERT INTO %s.%s VALUES %s
    ''' % (
        TEST_SCHEMA_NAME,
        TEST_TABLE_NAME,
        ', '.join(['(%d)' % i for i in range(2000)]),
    )


def get_create_scramble_str():
    return '''
        CREATE SCRAMBLE %s.%s FROM %s.%s BLOCKSIZE 200
    ''' % (
        TEST_SCHEMA_NAME,
        TEST_SCRAMBLE_NAME,
        TEST_SCHEMA_NAME,
        TEST_TABLE_NAME,
    )


def teardown_module(module):
    cur = redshift_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS "%s" CASCADE' % TEST_SCHEMA_NAME)
    cur.close()
    redshift_conn.close()


class TestClass:

    def test_simple_bypass_count(self):
        sql = 'select count(*) from {}.{}'.format(TEST_SCHEMA_NAME, TEST_TABLE_NAME)
        self.compare_bypass_query_result(sql)

    def test_simple_count(self):
        sql = 'select count(*) from {}.{}'.format(TEST_SCHEMA_NAME, TEST_TABLE_NAME)
        self.compare_approximate_query_result(sql)

    def test_verdict_block_count(self):
        sql = """bypass select min(verdictdbblock), max(verdictdbblock)
            from {}.{}""".format(TEST_SCHEMA_NAME, TEST_SCRAMBLE_NAME)
        result = verdict_conn.sql_raw_result(sql)
        types = result.types()
        # rows = [[x.toString() for x in row] for row in result.rows()]
        rows = result.rows()
        print(types)
        print('block counts: ' + str(rows))
        assert 0 == rows[0][0]
        assert 9 == rows[0][1]

    def compare_query_result(self, verdict_sql, regular_sql):
        result = verdict_conn.sql_raw_result(verdict_sql)
        types = result.types()
        rows = result.rows()
        print('verdict query: ' + str(verdict_sql))
        print('result: ' + str(rows))

        cur = redshift_conn.cursor()
        cur.execute(regular_sql)
        expected_rows = cur.fetchall()
        print('postgres query: ' + str(regular_sql))
        print('result: ' + str(expected_rows))
        cur.close()

        # Now test
        assert len(expected_rows) == len(rows)
        assert len(expected_rows) == result.rowcount
        for i in range(len(expected_rows)):
            expected_row = expected_rows[i]
            actual_row = rows[i]
            for j in range(len(expected_row)):
                self.compare_value(expected_row[j], actual_row[j], types[j])

    def compare_approximate_query_result(self, sql):
        result = verdict_conn.sql_raw_result(sql)
        rows = result.rows()

        cur = redshift_conn.cursor()
        cur.execute(sql)
        expected_rows = cur.fetchall()
        cur.close()

        # Now test
        assert len(expected_rows) == len(rows)
        assert len(expected_rows) == result.rowcount
        for i in range(len(expected_rows)):
            expected_row = expected_rows[i]
            actual_row = rows[i]
            for j in range(len(expected_row)):
                self.approximate_compare_value(expected_row[j], actual_row[j])

    def compare_bypass_query_result(self, sql):
        verdict_sql = 'bypass ' + sql
        self.compare_query_result(verdict_sql, sql)

    def compare_value(self, expected, actual, coltype):
        if coltype == 'decimal' and expected is not None:
            assert float(expected) == actual
        elif coltype == 'bytea' and expected is not None:
            assert expected.tobytes() == actual.tobytes()
        else:
            assert expected == actual

    def approximate_compare_value(self, expected, actual):
        assert expected is not None
        assert actual is not None
        assert expected * 1.2 >= actual
        assert expected * 0.8 <= actual


