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

test_schema = 'pyverdict_postgres_simple_query_test_schema' + str(uuid.uuid4())[:3]
test_table = 'test_table'
test_scramble = 'test_scramble'

pgres_conn = None
verdict_conn = None

def setup_module():
    global pgres_conn
    global verdict_conn

    host = 'localhost'
    port = 5432
    user = 'postgres'
    dbname = 'postgres'

    # create table and populate data
    pgres_conn = psycopg2_connect(host, port, user, dbname)
    pgres_conn.set_session(autocommit=True)

    cur = pgres_conn.cursor()

    cur.execute('DROP SCHEMA IF EXISTS "%s" CASCADE' % test_schema)
    cur.execute('CREATE SCHEMA IF NOT EXISTS "%s"' % test_schema)
    cur.execute(get_create_table_str())

    cur.execute(get_insert_into_table_str())
    cur.close()

    verdict_conn = verdict_connect(host, port, dbname, user)
    verdict_conn.sql(get_create_scramble_str())


def psycopg2_connect(host, port, dbname, usr):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=usr,
    )


def verdict_connect(host, port, dbname, user):
    connection_string = 'jdbc:postgresql://%s:%s/%s?user=%s' % (
        host,
        port,
        dbname,
        user,
    )

    return pyverdict.VerdictContext(connection_string)


def get_create_table_str():
    return '''
        CREATE TABLE IF NOT EXISTS %s.%s (
            intCol            integer
        )
    ''' % (
        test_schema,
        test_table,
    )


def get_insert_into_table_str():
    return '''
        INSERT INTO %s.%s VALUES %s
    ''' % (
        test_schema,
        test_table,
        ', '.join(['(%d)' % i for i in range(2000)]),
    )


def get_create_scramble_str():
    return '''
        CREATE SCRAMBLE %s.%s FROM %s.%s BLOCKSIZE 200
    ''' % (
        test_schema,
        test_scramble,
        test_schema,
        test_table,
    )


def teardown_module(module):
    cur = pgres_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS "%s" CASCADE' % test_schema)
    cur.close()
    pgres_conn.close()
    verdict_conn.close()


class TestClass:

    def test_simple_bypass_count(self):
        sql = 'select count(*) from {}.{}'.format(test_schema, test_table)
        self.compare_bypass_query_result(sql)

    def test_simple_count(self):
        sql = 'select count(*) from {}.{}'.format(test_schema, test_table)
        self.compare_approximate_query_result(sql)

    def test_verdict_block_count(self):
        sql = """bypass select min(verdictdbblock), max(verdictdbblock)
            from {}.{}""".format(test_schema, test_scramble)
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

        cur = pgres_conn.cursor()
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

        cur = pgres_conn.cursor()
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

