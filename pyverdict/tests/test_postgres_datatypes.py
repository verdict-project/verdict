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

test_schema = 'pyverdict_postgres_datatype_test_schema' + str(uuid.uuid4())[:3]
test_table = 'test_table'


def test_data_types():
    pgres_conn, verdict_conn = setup_sandbox()

    sql = 'SELECT * FROM "%s"."%s" ORDER BY bigintcol' % (
        test_schema,
        test_table,
    )

    cur = pgres_conn.cursor()
    cur.execute(sql)
    expected_rows = cur.fetchall()
    print(expected_rows)

    expected_types = [
        type(x) for x in expected_rows[0]
    ]
    print('expected types: %s' % (expected_types,))

    result = verdict_conn.sql_raw_result(sql)
    types = result.types()
    rows = result.rows()
    print(types)
    print(rows)
    print([[type(x) for x in row] for row in rows])

    # Now test
    assert len(expected_rows) == len(rows)
    assert len(expected_rows) == result.rowcount

    for i in range(len(expected_rows)):
        expected_row = expected_rows[i]
        actual_row = rows[i]
        for j in range(len(expected_row)):
            compare_value(expected_row[j], actual_row[j], types[j])

    tear_down(pgres_conn, verdict_conn)


def compare_value(expected, actual, coltype):
    if coltype == 'decimal' and expected is not None:
        assert float(expected) == actual
    elif coltype == 'bytea' and expected is not None:
        assert expected.tobytes() == actual.tobytes()
    elif coltype in ('timetz', 'timestamptz') and expected is not None:
        # The JDBC uses the client timezone which may not match the timezone
        # used by the DB.  To remedy this, we ignore the test.
        pass
    else:
        assert expected == actual


def setup_sandbox():
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

    cur.execute(get_create_table_str(test_schema, test_table))

    cur.execute(get_insert_real_data_str(test_schema, test_table))
    cur.execute(get_insert_sentinel_data_str(test_schema, test_table))

    # create verdict connection
    verdict_conn = verdict_connect(host, port, dbname, user)
    return (pgres_conn, verdict_conn)


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
            1, 1, '1', '1011', true, '((1.5,1), (2,2))', '1', '1234', '1234',
            '10', '((1,1),2)', '2018-12-31', 1.0, '88.99.0.0/16', 1,
            '{"2":1}', '{1,2,3}', '((1,1),(2,2))',
            '08002b:010203', '08002b:0102030405', '12.34', 1.0, '((1,1))', '(1,1)',
            '((1,1))', 1.0, 1, 1, 1, '1110', '2018-12-31 00:00:01', '2018-12-31 00:00:01',
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
            '<foo>bar</foo>',
            '1', 1, true, '1234', '1234', 1, 1, 1.0, 1.0, 1.0,
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


def tear_down(pgres_conn, verdict_conn):
    cur = pgres_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS "%s" CASCADE' % test_schema)
    verdict_conn.close()


def verdict_connect(host, port, dbname, user):
    connection_string = 'jdbc:postgresql://%s:%s/%s?user=%s' % (
        host,
        port,
        dbname,
        user,
    )

    return pyverdict.VerdictContext(connection_string)


def psycopg2_connect(host, port, dbname, usr):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=usr,
    )

