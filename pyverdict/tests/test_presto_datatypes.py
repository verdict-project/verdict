
from datetime import datetime, date
import os
import pyverdict
import prestodb

test_schema = 'pyverdict_datatype_test_schema'
test_table = 'pyverdict_datatype_test_table'


def test_data_types():
    (presto_conn, verdict_conn) = setup_sandbox()

    # result = verdict_conn.sql('select * from {}.{}'.format(test_schema, test_table))
    # types = result.types()
    # rows = result.rows()
    # print(rows)

    cur = presto_conn.cursor()
    cur.execute('select * from {}.{}'.format(test_schema, test_table))
    expected_rows = cur.fetchall()
    print(expected_rows)

    # Now test
    # assert len(expected_rows) == len(rows)
    # assert len(expected_rows) == result.rowcount
    #
    # for i in range(len(expected_rows)):
    #     expected_row = expected_rows[i]
    #     actual_row = rows[i]
    #     for j in range(len(expected_row)):
    #         compare_value(expected_row[j], actual_row[j])

    tear_down(presto_conn)


def compare_value(expected, actual):
    if isinstance(expected, bytes):
        if isinstance(actual, bytes):
            assert expected == actual
        else:
            assert int.from_bytes(expected, byteorder='big') == actual
    elif isinstance(expected, int) and isinstance(actual, date):
        # due to the limitation of the underlying MySQL JDBC driver, both year(2) and year(4) are
        # returned as the 'date' type; thus, we check the equality in this hacky way.
        assert expected % 100 == actual.year % 100
    else:
        assert expected == actual


def setup_sandbox():
    hostport = os.environ['VERDICTDB_TEST_PRESTO_HOST']
    host, port = hostport.split(':')
    port = int(port)
    catalog = os.environ['VERDICTDB_TEST_PRESTO_CATALOG']
    user = os.environ['VERDICTDB_TEST_PRESTO_USER']
    password = ''

    # create table and populate data
    presto_conn = presto_connect(host, port, user, catalog)
    cur = presto_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS {}'.format(test_schema))
    cur.fetchall()

    cur.execute('CREATE SCHEMA IF NOT EXISTS ' + test_schema)
    cur.fetchall()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS {}.{} (
          intCol              INT
        )""".format(test_schema, test_table)
        )
    cur.fetchall()
    # cur.execute("""
    #     INSERT INTO {}.{} VALUES (
    #       1, 2, 1, 1, 1, 1, 1, 1,
    #       1.0, 1.0, 1.0, 1.0, 1.0,
    #       '2018-12-31', '2018-12-31 01:00:00', '2018-12-31 00:00:01', '10:59:59',
    #       18, 2018, 'abc', 'abc', '10', '10',
    #       '10', 'a', '10', 'abc', '1110', 'abc', '1110', 'abc', '1', '2'
    #     )""".format(test_schema, test_table)
    #     )
    # cur.execute("""
    #     INSERT INTO {}.{} VALUES (
    #         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    #         NULL, NULL, NULL, NULL, NULL,
    #         NULL, NULL, NULL, NULL,
    #         NULL, NULL, NULL, NULL, NULL, NULL,
    #         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    #     )""".format(test_schema, test_table)
    #     )

    # create verdict connection
    # thispath = os.path.dirname(os.path.realpath(__file__))
    # mysql_jar = os.path.join(thispath, 'lib', 'mysql-connector-java-5.1.46.jar')
    verdict_conn = verdict_connect(host, port, catalog, user)

    return (presto_conn, verdict_conn)


def tear_down(presto_conn):
    cur = presto_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(test_schema, test_table))
    cur.fetchall()
    cur.execute('DROP SCHEMA IF EXISTS {}'.format(test_schema))
    cur.fetchall()


def verdict_connect(host, port, catalog, usr):
    connection_string = \
        'jdbc:presto://{:s}:{:d}/{:s}?user={:s}'.format(host, port, catalog, usr)
    return pyverdict.VerdictContext(connection_string)


def presto_connect(host, port, usr, catalog):
    return prestodb.dbapi.connect(
        host=host, port=port, user=usr, catalog=catalog)

