
import pymysql
import mysql_dump
import mysql_exec
import pytest


class AnsibleStub:

    def fail_json(self, *args, **kvargs):
        return False


@pytest.fixture()
def conn():
    return pymysql.connect(host='localhost',
                           db='g7-fportal',
                           user='root',
                           password='asdf10',
                           cursorclass=pymysql.cursors.DictCursor)

@pytest.fixture()
def db():
    convert_matrix = pymysql.converters.conversions
    convert_matrix[pymysql.FIELD_TYPE.BIT] = mysql_dump.convert_bit

    return mysql_dump.Database(host='localhost',
                  db='g7-fportal',
                  user='root',
                  password='asdf10',
                  conv=convert_matrix)


def test_conn(conn):
    assert conn


def test_Database(db):

    assert db
    assert str(db) == db._odbc
    tnames = db.tablenames()
    assert len(tnames) > 0
    assert tnames[0] != ""


def test_fetch(db):

    assert db

    res = db.fetch()
    assert res != None
    assert len(db.tables()) > 0


def test_dumpsql(db):

    assert db

    db.dumpsql('/tmp/test.sql')


def test_dump(db):

    assert db

    db.dump('/tmp/test.dmp')


def test_restore():

    db = mysql_dump.Database(host='localhost',
                  db='g7-fportal',
                  user='root',
                  password='asdf10')

    db.restore('/tmp/test.dmp')


def test_exec_query(conn):
    r = mysql_exec.my_query(conn, "SELECT VERSION();")
    assert r


def test_exec_exec(conn):

    m = AnsibleStub()
    try:
        mysql_exec.my_exec(conn, "/tmp/test.sql", m)
    except Exception as e:
        assert not e
