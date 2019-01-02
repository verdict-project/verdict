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

test_schema = 'pyverdict_postgres_datatype_test_schema' + str(uuid.uuid4())[:3]
test_table = 'test_table'


def test_data_types():
    presto_conn, verdict_conn = setup_sandbox()

    result = verdict_conn.sql_raw_result(
        'select * from {}.{}'.format(test_schema, test_table))
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
    tear_down(presto_conn)

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
    hostport = os.environ['VERDICTDB_TEST_POSTGRES_HOST']
    host, port = hostport.split(':')
    port = int(port)
    dbname = os.environ['VERDICTDB_TEST_POSTGRES_DBNAME']
    user = os.environ['VERDICTDB_TEST_PRESTO_USER']
    password = ''

    # create table and populate data
    presto_conn = presto_connect(host, port, user, dbname)
    cur = presto_conn.cursor()
    cur.fetchall()
    cur.execute('DROP SCHEMA IF EXISTS "%s" CASCADE' % test_schema)
    cur.fetchall()
    cur.execute('CREATE SCHEMA IF NOT EXISTS "%s"' % test_schema)
    cur.fetchall()

    cur.execute(get_create_table_str(test_schema, test_table))
    cur.fetchall()

    cur.execute(get_insert_real_data_str(test_schema, test_table))
    cur.fetchall()
    cur.execute(get_insert_sentinel_data_str(test_schema, test_table))
    cur.fetchall()

    # create verdict connection
    verdict_conn = verdict_connect(host, port, dbname, user)
    return (presto_conn, verdict_conn)

def get_create_table_str(test_schema, test_table):
    return '''
        CREATE TABLE "%s"."%s" (
            bigintCol      bigint,
            bigserialCol   bigserial,
            bitCol         bit(1),
            varbitCol      varbit(4),
            booleanCol     boolean,
            boxCol         box,
            byteaCol       bytea,
            charCol        char(4),
            varcharCol     varchar(4),
            cidrCol        cidr,
            circleCol      circle,
            dateCol        date,
            float8Col      float8,
            inetCol        inet,
            integerCol     integer,
            jsonCol        json,
            lineCol        line,
            lsegCol        lseg,
            macaddrCol     macaddr,
            macaddr8Col    macaddr8,
            moneyCol       money,
            numericCol     numeric(4,2),
            pathCol        path,
            pointCol       point,
            polygonCol     polygon,
            realCol        real,
            smallintCol    smallint,
            smallserialCol smallserial,
            serialCol      serial,
            textCol        text,
            timeCol        time,
            timestampCol   timestamp,
            uuidCol        uuid,
            xmlCol         xml,
            bitvaryCol     bit varying(1),
            int8Col        int8,
            boolCol        bool,
            characterCol   character(4),
            charactervCol  character varying(4),
            intCol         int,
            int4Col        int4,
            doublepCol     double precision,
            decimalCol     decimal(4,2),
            float4Col      float,
            int2Col        int2,
            serial2Col     serial2,
            serial4Col     serial4,
            timetzCol      timetz,
            timestamptzCol timestamptz,
            serial8Col     serial8
        )
    ''' % (
        test_schema,
        test_table,
    )

def get_insert_real_data_str(test_schema, test_table):
    return '''
        INSERT INTO "%s"."%s" VALUES (
            1, 1, '1', '1011', true, '((1,1), (2,2))', '1', '1234', '1234',
            '10', '((1,1),2)', '2018-12-31', 1.0, '88.99.0.0/16', 1,
            '{"2":1}', '{1,2,3}', '((1,1),(2,2))',
            '08002b:010203', '08002b:0102030405', '12.34', 1.0, '((1,1))', '(1,1)',
            '((1,1))', 1.0, 1, 1, 1, '1110', '2018-12-31 00:00:01', '2018-12-31 00:00:01',
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
            '<foo>bar</foo>',
            '1', 1, true, '1234', '1234', 1, 1, 1.0, 1.0, 1.0
            1, 1, 1, '2018-12-31 00:00:01', '2018-12-31 00:00:01', 1
        )
    ''' % (
        test_schema,
        test_table,
    )

def get_insert_sentinel_data_str(test_schema, test_table):
    return '''
        INSERT INTO "%s"."%s" VALUES (
            NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, 1, 1, NULL, NULL, NULL, NULL,
            NULL,
            NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, 1, 1, NULL, NULL, 1
        )
    ''' % (
        test_schema,
        test_table,
    )

def tear_down(presto_conn):
    cur = presto_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(test_schema, test_table))
    cur.fetchall()
    cur.execute('DROP SCHEMA IF EXISTS {}'.format(test_schema))
    cur.fetchall()

def verdict_connect(host, port, dbname, user):
    connection_string = 'jdbc:postgres://%s:%s/%s?user=%s' % (
        host,
        port,
        dbname,
        user,
    )

    return pyverdict.VerdictContext(connection_string)

def psycopg2_connect(host, port, usr, dbname):
    return psycopg2.connect(
        host=host,
        port=port,
        user=usr,
        dbname=dbname,
    )

