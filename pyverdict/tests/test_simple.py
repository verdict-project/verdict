import pyverdict as pv

def main():
    vc = pv.VerdictContext('jdbc:mysql://localhost:3306, 'root', '')
    vc.sql('CREATE SCHEMA pyverdict_simple_test')
    vc.sql('CREATE TABLE pyverdict_simple_test.test (id INT)')
    vc.sql('INSERT INTO pyverdict_simple_test.test SELECT 1')
    vc.sql('SELECT COUNT(1) from pyverdict_simple_test.test')
    result.parse()

if __name__ == '__main__':
    main()
