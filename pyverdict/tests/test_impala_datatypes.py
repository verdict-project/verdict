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
from datetime import datetime, date
import os
import pyverdict
import impala.dbapi
import uuid
import math

TEST_META_SCHEMA_NAME = 'pyverdict_impala_meta_schema'

TEST_SCHEMA_NAME = 'pyverdict_impala_datatype_test_schema' + str(uuid.uuid4())[:3]
TEST_TABLE_NAME = 'test_table'
NUM_COLS = 12

def test_data_types_real():
    _test_data(_get_insert_real_data_str())


def test_data_types_sentinel():
    _test_data(_get_insert_sentinel_data_str())


def _test_data(insertSQLStr):
    impala_conn, verdict_conn = setup_sandbox()

    cur = impala_conn.cursor()
    cur.execute(insertSQLStr)
    cur.close()

    _check_types_same(impala_conn, verdict_conn)

    tear_down(impala_conn, verdict_conn)


def _check_types_same(impala_conn, verdict_conn):
    result = verdict_conn.sql_raw_result(
        'SELECT * FROM %s.%s' % (
            TEST_SCHEMA_NAME,
            TEST_TABLE_NAME,
        )
    )

    types = result.types()
    rows = result.rows()
    print(rows)

    cur = impala_conn.cursor()
    cur.execute(
        'SELECT * FROM %s.%s' % (
            TEST_SCHEMA_NAME,
            TEST_TABLE_NAME,
        )
    )

    expected_rows = cur.fetchall()
    print(expected_rows)
    cur.close()

    expected_types = [
        type(x) for x in expected_rows[0]
    ]
    print('expected types: %s' % (expected_types,))

    # Now test
    assert len(expected_rows) == len(rows)
    assert len(expected_rows) == result.rowcount

    for i in range(len(expected_rows)):
        expected_row = expected_rows[i]
        actual_row = rows[i]
        for j in range(len(expected_row)):
            compare_value(expected_row[j], actual_row[j])


def compare_value(expected, actual):
    if isinstance(expected, bytes):
        if isinstance(actual, bytes):
            assert expected == actual
        else:
            assert int.from_bytes(expected, byteorder='big') == actual
    elif isinstance(expected, float):
        assert math.isclose(expected, actual, rel_tol=5e-05)
    else:
        assert expected == actual


def setup_sandbox():
    host, port = os.environ['VERDICTDB_TEST_IMPALA_HOST'].split(':')
    port = int(port)

    # create table and populate data
    impala_conn = impala_connect(host, port)
    cur = impala_conn.cursor()

    # Setup schema
    cur.execute('DROP SCHEMA IF EXISTS %s CASCADE' % TEST_SCHEMA_NAME)
    cur.execute('CREATE SCHEMA IF NOT EXISTS %s' % TEST_SCHEMA_NAME)

    # Setup table
    cur.execute(
        'DROP TABLE IF EXISTS %s.%s' % (
            TEST_SCHEMA_NAME,
            TEST_TABLE_NAME,
        )
    )
    cur.execute(_get_create_table_str())

    cur.close()

    verdict_conn = verdict_connect(host, port)

    return (impala_conn, verdict_conn)


def tear_down(impala_conn, verdict_conn):
    cur = impala_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS %s CASCADE' % TEST_SCHEMA_NAME)
    cur.close()
    impala_conn.close()
    verdict_conn.close()


def verdict_connect(host, port):
    connection_string = 'jdbc:impala://%s:%s' % (host, port)

    return pyverdict.VerdictContext(
        connection_string,
        verdictdbmetaschema=TEST_META_SCHEMA_NAME,
    )


def impala_connect(host, port):
    return impala.dbapi.connect(host=host, port=port)


def _get_create_table_str():
    return '''
        CREATE TABLE %s.%s (
            bigintCol         bigint,
            booleanCol        boolean,
            charCol           char(10),
            decimalCol        decimal(5,2),
            doubleCol         double,
            floatCol          float,
            realCol           real,
            smallintCol       smallint,
            stringCol         string,
            timestampCol      timestamp,
            tinyintCol        tinyint,
            varcharCol        varchar(10)
        )
    ''' % (
        TEST_SCHEMA_NAME,
        TEST_TABLE_NAME,
    )


def _get_insert_real_data_str():
    insert_data_tuple = (
        "6",                                 # bigint
        "true",                              # boolean
        "cast('john' as char(10))",          # char
        "123.45",                            # decimal
        "1000.001",                          # double
        "1000.001",                          # float
        "1000.001",                          # real
        "1",                                 # smallint
        "'michael'",                         # string
        "now()",                             # timestamp
        "2",                                 # tinyint
        "cast('jackson' as varchar(10))",    # varchar
    )

    return 'INSERT INTO %s.%s VALUES (%s)' % (
        TEST_SCHEMA_NAME,
        TEST_TABLE_NAME,
        ','.join(insert_data_tuple),
    )


def _get_insert_sentinel_data_str():
    insert_data_tuple = tuple('NULL' for _ in range(NUM_COLS))

    return 'INSERT INTO %s.%s VALUES (%s)' % (
        TEST_SCHEMA_NAME,
        TEST_TABLE_NAME,
        ','.join(insert_data_tuple),
    )

