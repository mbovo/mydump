
import pymysql
import mysql_dump

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

def test_Database():

    convert_matrix = pymysql.converters.conversions
    convert_matrix[pymysql.FIELD_TYPE.BIT] = mysql_dump.convert_bit


    db = mysql_dump.Database(host='localhost',
                  db='g7-fportal',
                  user='root',
                  password='asdf10',
                  conv=convert_matrix)

    assert db
    assert str(db) == db._odbc
    tnames = db.tablenames()
    assert len(tnames) > 0
    assert tnames[0] != ""

def test_fetch():

    db = mysql_dump.Database(host='localhost',
                  db='g7-fportal',
                  user='root',
                  password='asdf10')

    res = db.fetch()
    assert res != None
    assert len(db.tables()) > 0


def test_dumpsql():

    db = mysql_dump.Database(host='localhost',
                  db='g7-fportal',
                  user='root',
                  password='asdf10')

    db.dumpsql('/tmp/test.sql')


def test_dump():

    db = mysql_dump.Database(host='localhost',
                  db='g7-fportal',
                  user='root',
                  password='asdf10')

    db.dump('/tmp/test.dmp')

