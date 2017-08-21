import pymysql
import mysql_dump
import mysql_exec
import pytest


@pytest.fixture()
def conn():
    return pymysql.connect(host='localhost',
                           db='test',
                           user='root',
                           password='password',
                           cursorclass=pymysql.cursors.DictCursor)


@pytest.fixture()
def db():
    convert_matrix = pymysql.converters.conversions
    convert_matrix[pymysql.FIELD_TYPE.BIT] = mysql_dump.convert_bit

    return mysql_dump.Database(host='localhost',
                  db='test',
                  user='root',
                  password='password',
                  conv=convert_matrix)

slow = pytest.mark.skipif(
    not pytest.config.getoption("--long"),
    reason="need --long option to run"
)

class TestDump:

    def test_conn(self, conn):
        assert conn

    def test_Database(self, db ):

        assert db
        assert str(db) == db._odbc
        tnames = db.tablenames()
        assert len(tnames) > 0
        assert tnames[0] != ""

    def test_fetch(self, db):

        assert db

        res = db.fetch()
        assert res is not None
        assert len(db.tables()) > 0

    def test_dumpsql(self, db):

        assert db

        db.dumpsql('/tmp/test.sql')

    @slow
    def test_dump(self, db):

        assert db

        db.dump('/tmp/test.dmp')

    @slow
    def test_restore(self):

        db = mysql_dump.Database(host='localhost',
                      db='test',
                      user='root',
                      password='password')

        print db.restore('/tmp/test.dmp')


class TestExec:

    def test_query(self, conn):
        r = mysql_exec.my_query(conn, "SELECT VERSION();")
        assert r

    def test_exec(self, conn):

        with open('/tmp/test.sql', 'w') as fp:
            fp.write('SELECT * FROM mysql.user;')

        r = mysql_exec.my_exec(conn, '/tmp/test.sql')
        print r
        assert r
