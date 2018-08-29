import pyverdict
import pymysql

def test_count():
    conn_mysql = mysql_connect('localhost', 3306, 'root', '')
    cur = conn_mysql.cursor()
    cur.execute('DROP SCHEMA IF EXISTS pyverdict_simple_test')
    cur.execute('CREATE SCHEMA IF NOT EXISTS pyverdict_simple_test')
    cur.execute('CREATE TABLE IF NOT EXISTS pyverdict_simple_test.test (id INT)')
    cur.execute('INSERT INTO pyverdict_simple_test.test SELECT 1')

    conn_verdict = verdict_connect('localhost', 3306, 'root', '', 'tests/lib/mysql-connector-java-5.1.46.jar')
    result = conn_verdict.sql('SELECT COUNT(1) from pyverdict_simple_test.test')
    assert result.fetch_one() == 1
    cur.execute('DROP SCHEMA IF EXISTS pyverdict_simple_test')
    cur.close()
    conn_mysql.close()

def verdict_connect(host, port, usr, pwd, jdbc_driver):
    connect_string = 'jdbc:mysql://{:s}:{:d}'.format(host, port)
    return pyverdict.VerdictContext(connect_string, usr, pwd, jdbc_driver)

def mysql_connect(host, port, usr, pwd):
    return pymysql.connect(host=host, port=port, user=usr, passwd=pwd, autocommit=True)
