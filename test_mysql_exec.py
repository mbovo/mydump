
import pymysql
import mysql_exec

class AnsibleStub:

    def fail_json(self, *args, **kvargs):
        return False


conn = None


def test_conn():
    global conn
    conn = pymysql.connect(host='localhost',
                           db='g7-fportal',
                           user='root',
                           password='asdf10',
                           cursorclass=pymysql.cursors.DictCursor)
    assert conn

def test_query():
    global conn
    r = mysql_exec.my_query(conn, "SELECT VERSION();")
    assert r

def test_exec():
    global conn#

    m = AnsibleStub()
    try:
        mysql_exec.my_exec(conn, "/tmp/test.sql", m)
    except Exception as e:
        assert not e

