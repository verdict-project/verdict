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
from datetime import datetime, date, timezone
import os
import pyverdict
import prestodb
import uuid

test_schema = 'pyverdict_presto_datatype_test_schema' + str(uuid.uuid4())[:3]
test_table = 'test_table'


def test_data_types():
    (presto_conn, verdict_conn) = setup_sandbox()

    result = verdict_conn.sql_raw_result(
        'select * from {}.{} order by tinyintCol'.format(test_schema, test_table))
    types = result.types()
    rows = result.rows()
    print(types)
    print(rows)
    print([[type(x) for x in row] for row in rows])

    cur = presto_conn.cursor()
    cur.execute('select * from {}.{} order by tinyintCol'.format(test_schema, test_table))
    expected_rows = cur.fetchall()
    print(expected_rows)

    # Now test
    assert len(expected_rows) == len(rows)
    assert len(expected_rows) == result.rowcount

    for i in range(len(expected_rows)):
        expected_row = expected_rows[i]
        actual_row = rows[i]
        for j in range(len(expected_row)):
            compare_value(expected_row[j], actual_row[j], types[j])
    tear_down(presto_conn, verdict_conn)

def compare_value(expected, actual, coltype):
    if coltype == 'decimal' and expected is not None:
        assert float(expected) == actual
    elif coltype == "timestamp" and expected is not None:
        # Presto's official Python driver follows the server's time zone while
        # JDBC driver follows the client's time zone.
        # To reconcile this, we change the Python driver's value to the client
        # time zone. We assume that the server is using 'UTC'.
        parsed = datetime.strptime(expected, "%Y-%m-%d %H:%M:%S.%f")
        local = parsed.replace(tzinfo=timezone.utc).astimezone(tz=None)
        expected = local.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        assert expected == actual
    else:
        assert expected == actual

def setup_sandbox():
    '''
    We test the data types defined here: https://prestodb.io/docs/current/language/types.html
    except for JSON, INTERVAL, ARRAY, MAP, ROW, IPADDRESS

    Note:
    1. "time with time zone" is not supported: "Unsupported Hive type: time with time zone"
    2. "timestamp with time zone" is not supported: Unsupported Hive type: timestamp with time zone
    '''
    hostport = os.environ['VERDICTDB_TEST_PRESTO_HOST']
    host, port = hostport.split(':')
    port = int(port)
    catalog = os.environ['VERDICTDB_TEST_PRESTO_CATALOG']
    user = os.environ['VERDICTDB_TEST_PRESTO_USER']
    password = ''

    # create table and populate data
    presto_conn = presto_connect(host, port, user, catalog)
    cur = presto_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(test_schema, test_table))
    cur.fetchall()
    cur.execute('DROP SCHEMA IF EXISTS {}'.format(test_schema))
    cur.fetchall()
    cur.execute('CREATE SCHEMA IF NOT EXISTS ' + test_schema)
    cur.fetchall()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            tinyintCol          TINYINT,
            boolCol             BOOLEAN,
            smallintCol         SMALLINT,
            integerCol          INTEGER,
            bigintCol           BIGINT,
            decimalCol          DECIMAL(4,2),
            realCol             REAL,
            doubleCol           DOUBLE,
            dateCol             DATE,
            timestampCol        TIMESTAMP,
            charCol             CHAR(4),
            varcharCol          VARCHAR(4)
        )""".format(test_schema, test_table)
        )
    cur.fetchall()

    cur.execute("""
        INSERT INTO {}.{} VALUES (
            cast(1 as tinyint), true,
            cast(2 as smallint),
            cast(3 as integer),
            cast(4 as bigint),
            cast(5.0 as decimal(4,2)),
            cast(1.0 as real),
            cast(1.0 as double),
            cast('2018-12-31' as date),
            timestamp '2018-12-31 00:00:01',
            cast('ab' as char(4)),
            cast('abcd' as varchar(4))
        )""".format(test_schema, test_table)
        )
    cur.fetchall()

    cur.execute("""
        INSERT INTO {}.{} VALUES (
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL
        )""".format(test_schema, test_table)
        )
    cur.fetchall()

    # create verdict connection
    # thispath = os.path.dirname(os.path.realpath(__file__))
    # mysql_jar = os.path.join(thispath, 'lib', 'mysql-connector-java-5.1.46.jar')
    verdict_conn = verdict_connect(host, port, catalog, user)
    return (presto_conn, verdict_conn)

def tear_down(presto_conn, verdict_conn):
    cur = presto_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(test_schema, test_table))
    cur.fetchall()
    cur.execute('DROP SCHEMA IF EXISTS {}'.format(test_schema))
    cur.fetchall()
    verdict_conn.close()

def verdict_connect(host, port, catalog, usr):
    connection_string = \
        'jdbc:presto://{:s}:{:d}/{:s}?user={:s}'.format(host, port, catalog, usr)
    return pyverdict.VerdictContext(connection_string)

def presto_connect(host, port, usr, catalog):
    return prestodb.dbapi.connect(
        host=host, port=port, user=usr, catalog=catalog)

