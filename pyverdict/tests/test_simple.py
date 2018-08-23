from pyverdict.jvm import JVM
from pyverdict.resultset import ResultSet
from pyverdict.verdictcontext import VerdictContext

def main():
    jvm = JVM() # call this only once
    gateway = jvm.connect()
    conn = VerdictContext(gateway, 'localhost', 3306, 'instacart', 'root', '')
    result = conn.sql('select count(1) from instacart.orders_joined')
    result.parse()
    jvm.stop()

if __name__ == '__main__':
    main()
