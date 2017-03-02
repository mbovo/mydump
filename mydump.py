#!/usr/bin/env python2

import argparse
import datetime
import os
import pickle
import sys
import types
import struct

import pymysql

VERBOSE = 0

SQL_ST_PRE = """/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;"""

SQL_ST_POST = """/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;"""

SQL_DROP_ST_PRE = """
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
"""

SQL_DROP_ST_POST = None

SQL_INSERT_ST_PRE = """
LOCK TABLES `{}` WRITE;
/*!40000 ALTER TABLE `{}` DISABLE KEYS */;"""

SQL_INSERT_ST_POST = """
/*!40000 ALTER TABLE `{}` ENABLE KEYS */;
UNLOCK TABLES;
/*!40101 SET character_set_client = @saved_cs_client */;
"""


class Database:

    # a mysql connection object
    _conn = None
    _dbname = ""
    _odbc = ""
    _dirname = ""
    _tables = dict()
    _charset = "utf8"

    # will be converted into tuple() after __init__
    _tablenames = list()

    # define an argument subset of pymysql Connect object
    def __init__(self, host=None, user=None, password="",
                 db=None, port=0, unix_socket=None,
                 charset='utf8', sql_mode=None,
                 conv=None, use_unicode=None,
                 cursorclass=pymysql.cursors.DictCursor, ssl=None):

        self._conn = pymysql.connect(host=host,
                                     user=user,
                                     password=password,
                                     db=db,
                                     charset=charset,
                                     sql_mode=sql_mode,
                                     conv=conv,
                                     use_unicode=use_unicode,
                                     ssl=ssl,
                                     cursorclass=cursorclass)
        self._dbname = db
        self._odbc = "{}:{}@{}/{}".format(user, password, host, db)
        self._tablenames = list()
        self._tables = dict()
        self._charset = charset
        self._dirname = ""

        cur = self._conn.cursor()
        cur.execute("SHOW TABLES")

        for i in cur:
            self._tablenames.append(i.values()[0])
        self._tablenames = tuple(self._tablenames)

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return unicode(self._odbc)

    def tablenames(self):
        return self._tablenames

    def tables(self):
        return self._tables

    def __contains__(self, item):
        if item in self._tablenames:
            return True
        return False

    def __iter__(self):
        return self._tablenames.__iter__()

    def __getitem__(self, item):
        if item not in self:
            raise ValueError("Table {} not found in database".format(item))
        if item not in self._tables:
            table = Table(item, self._conn)
            self._tables[item] = table
        else:
            table = self._tables[item]
        return table

    def __delitem__(self, key):
        pass

    def __len__(self):
        return len(self._tablenames)

    def __fetch_n_dump(self, tablelist=None, exclude=False, dump=False, refetch=True, dirname=None ):

        # use the whole list  or choose
        if not tablelist:
            tablelist = self._tablenames
        else:
            if exclude:
                # exclude table listed in tablelist
                tablelist = list(set(self._tablenames) - set(tablelist))
            else:
                # include only table listed in tablelist
                tablelist = list(set(self._tablenames) & set(tablelist))

        # force loading data
        for tablename in tablelist:
            if refetch:
                self[tablename]
            if dump and dirname:
                table = self._tables[tablename]
                table.dump(os.path.join(dirname, tablename) + ".obj")

        # return table loaded for real
        return len(self._tables), tablelist

    def fetch(self, tablelist=None, exclude=False):
        return self.__fetch_n_dump(tablelist, exclude)

    def dump(self, dirname=None, tablelist=None, exclude=False, refetch=True):
        mkdir(dirname)
        self.__fetch_n_dump(tablelist, exclude, dump=True, refetch=refetch, dirname=dirname)

    def restore(self, path=None, tablelist=None, exclude=False):
        # load list of filename
        checkdir(path)
        tables_on_fs = list()
        for dirname, dirnames, filenames in os.walk(path):
            for filename in filenames:
                tables_on_fs.append(filename[:-4])

        if not tablelist:
            tablelist = tables_on_fs
        else:
            if exclude:
                tablelist = list(set(tables_on_fs) - set(tablelist))
            else:
                tablelist = list(set(tables_on_fs) & set(tablelist))

        for tname in tablelist:
            table = Table()
            table = table.load(os.path.join(dirname, tname)+".obj")

            self._tables[tname] = table
            cur = self._conn.cursor()
            if SQL_DROP_ST_PRE:
                cur.execute(SQL_DROP_ST_PRE)

            try:
                cur.execute(u"DROP TABLE IF EXISTS `{}`;".format(tname))
            except pymysql.err.MySQLError as e:
                print "Unable to DROP table `{} {}` exception raised".format(tname, e)
                continue
            try:
                cur.execute(str(table))
            except pymysql.err.MySQLError as e:
                print "Unable to CREATE table `{} {}` exception raised".format(tname, e)
                continue

            if SQL_DROP_ST_POST:
                cur.execute(SQL_DROP_ST_POST)

            # restore each row
            if SQL_INSERT_ST_PRE:
                cur.execute(SQL_INSERT_ST_PRE.format(tname, tname))

            for row in table:
                try:
                    cur.execute(str(row))
                except pymysql.err.MySQLError as e:
                    print "Unable to INSERT INTO table `{} {}` exception raised".format(tname, e)
                    print "-"*10
                    print row
                    print "-"*10
                    continue

            if SQL_INSERT_ST_POST:
                cur.execute(SQL_INSERT_ST_POST.format(tname, tname))

            _ = cur.fetchall()

        return len(self._tables), tablelist


class Table:

    _tablename = None
    _rows = list()
    _desc = dict()
    _ddl = dict()
    _charset = 'utf-8'

    def __init__(self, tablename="", conn=None, charset=None):
        if not conn or not isinstance(conn, pymysql.connections.Connection):
            return

        if charset:
            self._charset = charset

        self._tablename = tablename
        self._desc = self._get_desc(conn)
        self._ddl = self._get_ddl(conn)
        self._rows = []

        for row in self._get_rows(conn):
            self._rows.append(Row(row, self._desc, tablename))

    def __str__(self):
        return self._ddl[u'Create Table']

    def __unicode__(self):
        return unicode(self._ddl[u'Create Table']).encode(self._charset)

    def __eq__(self, other):
        # This is very cost impacting
        if not isinstance(other, Table):
            return False
        if len(other.rows()) != len(self._rows):
            return False
        for row in other.rows():
            if row not in self._rows:
                return False
        return True

    def __getitem__(self, item):
        if item < len(self._rows):
            return self._rows[item]

    def __len__(self):
        return len(self._rows)

    def __iter__(self):
        return self._rows.__iter__()

    def rows(self):
        return self._rows

    def name(self):
        return self._tablename

    def _get_ddl(self, conn=None):
        """
        Retrieve table DDL
        :param con: connection object
        :param tablename: table name as string
        :return: dict object Create:Statements
        """
        tablename = self._tablename
        cur = conn.cursor()
        cur.execute("SHOW CREATE TABLE `" + tablename + "`;")
        return cur.fetchone()

    def _get_desc(self, conn=None):
        """
        Retrieve DESC of a table
        :param con: Connection Object
        :param tablename: String database name
        :return: dict with association Field:Type
        """
        tablename = self._tablename
        cur = conn.cursor()
        cur.execute("DESC `" + tablename + "`;")

        desc = dict()
        for field in cur.fetchall():
            desc[field[u'Field']] = field[u'Type']
        return desc

    def _get_rows(self,conn):
        tablename = self._tablename
        fields = self._desc

        query = "SELECT "
        first = True
        for field in fields.keys():
            query += "," if first is False else ""
            first = False

            if fields[field] in ("blob", "longblob", "mediumblob"):
                query += "HEX(`" + field + "`) AS " + field
            else:
                query += "`" + field + "`"

        query += " FROM `" + tablename + "`;"

        cur = conn.cursor()
        cur.execute(query)

        return cur.fetchall()

    def dump(self, filename):
        """
        Dump a table to a file using python pickle
        :param filename: target filename
        :param obj: table object
        :return:
        """
        try:
            f = open(filename, "wb")
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)
            f.close()
        except pickle.UnpicklingError as e:
            print u"ERROR: Unable to save object to file [{}] : {}".format(filename, repr(e))

    def load(self, filename):
        """
        Load a table from a file using python pickle
        :param filename: target filename
        :return: table object
        """
        try:
            f = open(filename, "rb")
            self = pickle.load(f)
            f.close()
        except Exception as e:
            print u"ERROR: Unable to load object from file: [{}] : {}".format(filename, repr(e))
            sys.exit(20)
        return self


class Row:

    _element = dict()
    _desc = dict()
    _tablename = None
    _charset = "utf-8"

    def __init__(self, row=dict(), desc=dict(), tablename="", charset="utf-8"):
        self._element = row
        self._desc = desc
        self._tablename = tablename
        self._charset = charset

    def __str__(self):
        return self._get_ddl().encode(self._charset)

    def __contains__(self, item):
        return item in self._element

    def __eq__(self, other):
        if not isinstance(other, Row):
            return False
        if cmp(self.dict(), other.dict()) == 0:
            return True
        return False

    def __getitem__(self, item):
        if item in self:
            return self._element[item]

    def dict(self):
        return self._element

    def __unicode__(self):
        return self._get_ddl().encode(self._charset)

    def __len__(self):
        return len(self._element)

    def __iter__(self):
        return self._element.__iter__()

    def _get_ddl(self):
        query = ""
        fields = self._desc
        query += "INSERT INTO `" + self._tablename + "` ( `" + "`,`".join(fields.keys()) + "` ) VALUES ("

        if int(VERBOSE) >= 2:
            print u"DEBUG:\t      Row {} / {}".format(ncur, tot)

        first = True
        for field in fields.keys():
            query += "," if first is False else ""
            first = False

            if int(VERBOSE) >= 3:
                print u"DEBUG:\t\t   Field: {0:30}  Val: {1}".format(field, self._element[field]) \
                    .encode('utf-8', errors='ignore')

            if fields[field] in ("blob", "longblob", "mediumblob"):
                query += "UNHEX('" + self._element[field] + u"')"
            elif fields[field] in "date":
                query += pymysql.converters.escape_date(self._element[field])
            else:
                val = self._element[field]
                if isinstance(val, types.NoneType):
                    query += pymysql.converters.escape_None(val)
                elif isinstance(val, (int, long)):
                    query += pymysql.converters.escape_int(val)
                elif isinstance(val, float):
                    query += pymysql.converters.escape_float(val)
                elif isinstance(val, bool):
                    query += pymysql.converters.escape_bool(val)
                elif isinstance(val, datetime.datetime):
                    query += "'" + unicode(val) + "'"
                elif isinstance(val, types.UnicodeType):
                    query += pymysql.converters.escape_unicode(val)

        query += ");"

        if int(VERBOSE) >= 4:
            # noinspection PyBroadException
            try:
                print u"DEBUG:\n {}".format(query).encode(self._charset, errors='ignore')
            except:
                pass
        return query


def mkdir(dirname):
    try:
        if os.path.isdir(dirname):
            if int(VERBOSE)>0:
                print u"DEBUG:\tTarget directory already exists {}".format(dirname)
            return False
        os.mkdir(dirname)
        return True
    except OSError as e:
        print u"ERROR:\tError creating directory on {} : {}".format(dirname, e.strerror)
        sys.exit(4)


def checkdir(dirname):
    try:
        if os.path.isdir(dirname):
            return True
        else:
            return False
    except OSError as e:
        print u"ERROR:\t Unable to check if {} is a directory: {}".format(dirname, e.strerror)


def prepare(dbname, prefix="backup_", usedate=False, fulldir=None):
    """

    Create target directory where store dump files

    :param dbname: Database name
    :param prefix: directory prefix
    :param usedate: boolean, use timestamp
    :param fulldir: fulldir name instead of creation
    :return: directory path as string
    """
    if fulldir:
        dirname = fulldir
    else:
        dirname = os.getcwd() + "/" + prefix + dbname
        if usedate:
            now = datetime.datetime.now()
            dirname += "_" + now.strftime("%Y-%m-%d_%H-%M")

    if int(VERBOSE) > 0:
        print u"DEBUG: Target directory: {}".format(dirname)
    return dirname


def convert_bit(b):
    b = "\x00" * (8 - len(b)) + b # pad w/ zeroes
    return struct.unpack(">Q", b)[0]


def _build_parser():
    parser = argparse.ArgumentParser(prog="mydump")

    g1 = parser.add_mutually_exclusive_group(required=False)
    g1.add_argument("-d", "--dump", action="store_true", default=True, help="Dump database to disk (default)")
    g1.add_argument("-r", "--restore", action="store_true", default=False, help="Restore database from disk")

    parser.add_argument("-H", "--host", default="localhost", help="Database host (default: localhost)")
    parser.add_argument("-c", "--charset", default="utf8", help="Database and output charset (default: UTF8)")
    parser.add_argument("-o", "--target", default=None, help="Target directory (default: $prefix_$dbname)")
    parser.add_argument("-P", "--prefix", default="backup_", help="Prefix of destination directory (default: backup_ )")
    parser.add_argument("-t", "--timestamp", action="store_true",
                        help="Use timestamp in destination directory (default: false )")

    g2 = parser.add_mutually_exclusive_group(required=False)
    g2.add_argument("-i", "--include", action="store_false", help="Explicitly dump only the specified tables")
    g2.add_argument("-e", "--exclude", action="store_true", help="Exclude specified tables from dump")

    parser.add_argument("-u", "--user", help="Database Username", required=True)
    parser.add_argument("-p", "--password", help="Database Password", required=True)

    parser.add_argument("-v", "--verbose", default=0, help="Be Verbose")

    parser.add_argument("dbname", nargs=1, type=str, help="Database name")

    parser.add_argument("tables", nargs="*", type=unicode, default=None, help="Table name to include in dump "
                                                                              "(default everything)")

    assert isinstance(parser, object)
    return parser


def _parse_command(parser=None):
    global VERBOSE
    assert isinstance(parser, argparse.ArgumentParser)
    args = parser.parse_args()

#    if len(sys.argv) < 1:
#        parse.print_help()
#        return 1
    args.dbname = args.dbname[0]

    VERBOSE = args.verbose

    # patching for bit conversion
    convert_matrix = pymysql.converters.conversions
    convert_matrix[pymysql.FIELD_TYPE.BIT] = convert_bit

    try:
        db = Database(host=args.host,
                      db=args.dbname,
                      user=args.user,
                      password=args.password,
                      charset=args.charset,
                      conv=convert_matrix)
    except pymysql.err.MySQLError as e:
        print e
        os.abort()


    path = prepare(args.dbname, args.prefix, args.timestamp, args.target)

    if args.restore:
        db.restore(path=path, tablelist=args.tables, exclude=args.exclude)
    if args.dump:
        db.dump(dirname=path,tablelist=args.tables,exclude=args.exclude)


from ansible.module_utils.basic import *

fields = {
    "action": {
        "default": "dump",
        "choices": ["dump", "restore"],
        "type": "str"
    },
    "db": {"required": True, "type": "str"},
    "host": {"required": False, "default": "localhost", "type": "str"},
    "user": {"required": True, "type": "str"},
    "path": {"required": True, "type": "str"},
    "password": {"required": True, "type": "str"},
    "charset": {"required": False, "default": "utf8", "type": "str"},
    "prefix": {"required": False, "default": "backup_", "type": "str"},
    "timestamp": {"required": False, "default": False, "type": "bool"},
    "exclude": {"required": False, "default": False, "type": "bool"},
    "tables": {"required": False, "default": None, "type": "list"}
}


def main():

    if len(sys.argv) > 1:
        _parse_command(_build_parser())
        return

    m = AnsibleModule(argument_spec=fields)

    convert_matrix = pymysql.converters.conversions
    convert_matrix[pymysql.FIELD_TYPE.BIT] = convert_bit

    args = m.params

    try:
        db = Database(host=args['host'],
                      db=args['db'],
                      user=args['user'],
                      password=args['password'],
                      charset=args['charset'],
                      conv=convert_matrix)
    except pymysql.err.MySQLError as e:
        m.fail_json(msg=str(e))

    path = prepare(args['db'], args['prefix'], args['timestamp'], args['path'])

    try:
        if "restore" in args['action']:
            db.restore(path=path, tablelist=args['tables'], exclude=args['exclude'])
        if "dump" in args['action']:
            db.dump(dirname=path, tablelist=args['tables'], exclude=args['exclude'])
    except pymysql.err.MySQLError as e:
        m.fail_json(msg=str(e))

    m.exit_json(changed=True, meta=m.params)


if __name__ == "__main__":
    #exit(_parse_command(_build_parser()))
    main()
