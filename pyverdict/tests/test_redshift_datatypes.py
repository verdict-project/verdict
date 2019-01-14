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
import psycopg2
import uuid
import math

REDSHIFT_DBNAME = 'dev'
TEST_SCHEMA_NAME = 'pyverdict_redshift_datatype_test_schema' + str(uuid.uuid4())[:3]
TEST_TABLE_NAME = 'test_table'
NUM_COLS = 27

def test_data_types_real():
    _test_data(_get_insert_real_data_str())


def test_data_types_sentinel():
    _test_data(_get_insert_sentinel_data_str())


def _test_data(insertSQLStr):
    host, port = os.environ['VERDICTDB_TEST_REDSHIFT_ENDPOINT'].split(':')
    port = int(port)

    username = os.environ['VERDICTDB_TEST_REDSHIFT_USER']
    password = os.environ['VERDICTDB_TEST_REDSHIFT_PASSWORD']

    redshift_conn = setup_sandbox(host, port, username, password)

    verdict_conn = None
    try:
        verdict_conn = verdict_connect(host, port, username, password)

        cur = redshift_conn.cursor()
        cur.execute(insertSQLStr)
        cur.close()

        _check_types_same(redshift_conn, verdict_conn)

    finally:
        tear_down(redshift_conn, verdict_conn)


def _check_types_same(redshift_conn, verdict_conn):
    result = verdict_conn.sql_raw_result(
        'SELECT * FROM %s.%s' % (
            TEST_SCHEMA_NAME,
            TEST_TABLE_NAME,
        )
    )

    types = result.types()
    rows = result.rows()
    print(rows)

    cur = redshift_conn.cursor()
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
    print('Type names: %s' % (types))
    print('expected types: %s' % (expected_types,))

    # Now test
    assert len(expected_rows) == len(rows)
    assert len(expected_rows) == result.rowcount

    for i in range(len(expected_rows)):
        expected_row = expected_rows[i]
        actual_row = rows[i]
        for j in range(len(expected_row)):
            compare_value(expected_row[j], actual_row[j], types[i])


def compare_value(expected, actual, col_type_name):
    if isinstance(expected, bytes):
        if isinstance(actual, bytes):
            assert expected == actual
        else:
            assert int.from_bytes(expected, byteorder='big') == actual
    elif isinstance(expected, float):
        assert math.isclose(expected, actual, rel_tol=5e-05)
    elif col_type_name in ('timetz', 'timestamptz'):
        # We must depend on the timezone returned by the JDBC, so simply
        # ignore equality test in this case.
        pass
    else:
        assert expected == actual


def setup_sandbox(host, port, username, password):
    # create table and populate data
    redshift_conn = redshift_connect(host, port, username, password)
    redshift_conn.set_session(autocommit=True)

    cur = redshift_conn.cursor()

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

    return redshift_conn


def tear_down(redshift_conn, verdict_conn):
    cur = redshift_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS %s CASCADE' % TEST_SCHEMA_NAME)
    cur.close()
    redshift_conn.close()

    if verdict_conn is not None:
        verdict_conn.close()


def verdict_connect(host, port, username, password):
    return pyverdict.VerdictContext.new_redshift_context(
        host,
        port,
        REDSHIFT_DBNAME,
        username,
        password,
    )


def redshift_connect(host, port, username, password):
    return psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        dbname=REDSHIFT_DBNAME,
    )


def _get_create_table_str():
    return '''
        CREATE TABLE "%s"."%s" (
            smallintCol           SMALLINT,
            int2Col               INT2,
            integerCol            INTEGER,
            intCol                INT,
            int4Col               INT4,
            bigintCol             BIGINT,
            decimalCol            DECIMAL(5,2),
            numericCol            NUMERIC(5,2),
            realCol               REAL,
            float4Col             FLOAT4,
            doublePrecCol         DOUBLE PRECISION,
            float8Col             FLOAT8,
            floatCol              FLOAT,
            booleanCol            BOOLEAN,
            boolCol               BOOL,
            charCol               CHAR(10),
            characterCol          CHARACTER(10),
            ncharCol              NCHAR(10),
            bpcharCol             BPCHAR,
            varcharCol            VARCHAR(10),
            charactervarCol       CHARACTER VARYING(10),
            nvarcharCol           NVARCHAR(10),
            textCol               TEXT,
            dateCol               DATE,
            timestampCol          TIMESTAMP,
            timestampwtzCol       TIMESTAMP WITHOUT TIME ZONE,
            timestamptzCol        TIMESTAMPTZ,
            timestamptzCol2       TIMESTAMP WITH TIME ZONE
        )
    ''' % (
        TEST_SCHEMA_NAME,
        TEST_TABLE_NAME,
    )


def _get_insert_real_data_str():
    insert_data_tuple = (
        "1",                           # smallint
        "2",                           # int2
        "3",                           # integer
        "4",                           # int
        "5",                           # int4
        "6",                           # bigint
        "123.45",                      # decimal
        "-123.45",                     # numeric
        "1000.001",                    # real
        "1000.001",                    # float4
        "1000.001",                    # double precision
        "1000.001",                    # float8
        "1000.001",                    # float
        "true",                        # boolean
        "false",                       # bool
        "'john'",                      # char
        "'kim'",                       # character
        "'michael'",                   # nchar
        "'jackson'",                   # bpchar
        "'yo'",                        # varchar
        "'hey'",                       # character varying
        "'sup'",                       # nvarchar
        "'sometext'",                  # text
        "'2018-12-31'",                # date
        "'2018-12-31 11:22:33'",       # timestamp
        "'2018-12-31 11:22:33'",       # timestamp without time zone
        "'2018-12-31 11:22:33'",       # timestamptz
        "'2018-12-31 11:22:33'",       # timestamp with time zone
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

