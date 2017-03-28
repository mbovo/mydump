#!/usr/bin/env python2
# -*- coding: UTF-8 -*-

import pickle
import sys
import struct
import tarfile
import os
import os.path
import tempfile
import shutil
import time
import pymysql
import datetime
import types

from ansible.module_utils.basic import AnsibleModule

if sys.version_info > (2, 7):
    import argparse

"""
Pure python implementation of mysqldump utility as ansible module
(and standalone script for python>2.7) This code uses pymysql 3d party library
"""

__author__ = "Manuel Bovo <mbovo@facilitylive.com>"
__license__ = "MIT"
__version__ = "2.4.1"

VERBOSE = 0


SQL_ST_PRE = """

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
"""

SQL_ST_POST = """
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump Completed on {0}
"""

SQL_DROP_ST_PRE = """
--
-- Table structure for table `{0}`
--

"""

SQL_CREATE_ST_PRE = """
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
"""

SQL_INSERT_ST_PRE = """
--
-- Dumping data for table `{0}`
--
LOCK TABLES `{0}` WRITE;
/*!40000 ALTER TABLE `{0}` DISABLE KEYS */;
"""

SQL_INSERT_ST_POST = """
/*!40000 ALTER TABLE `{0}` ENABLE KEYS */;
UNLOCK TABLES;
/*!40101 SET character_set_client = @saved_cs_client */;
"""


class Database:

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
                                     port=port,
                                     unix_socket=unix_socket,
                                     cursorclass=cursorclass)
        self._dbname = db
        self._odbc = "{0}:{1}@{2}/{3}".format(user, password, host, db)
        self._tablenames = list()
        self._tables = dict()
        self._charset = charset
        self._dirname = ""

        cur = self._conn.cursor()
        cur.execute("SELECT VERSION();")

        self.server_version = cur.fetchall()[0]['VERSION()']

        global SQL_ST_PRE
        SQL_ST_PRE = """-- mysql_dump - pure python mysql dump {0}
--
-- Host: {1}    Database: {2}
-- ------------------------------------------------------
-- Server version     {3}
""".format(__version__, host, db, self.server_version) + SQL_ST_PRE

        cur.execute("SHOW TABLES")

        for i in cur:
            self._tablenames.append(i.values()[0])
        self._tablenames = tuple(self._tablenames)

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return unicode(self._odbc)

    def tablenames(self):
        return self._tablenames[:]

    def tables(self):
        return self._tables.copy()

    def __contains__(self, item):
        if item in self._tablenames:
            return True
        return False

    def __iter__(self):
        return self._tablenames.__iter__()

    def __getitem__(self, item):
        if item not in self:
            raise ValueError("Table {0} not found in database".format(item))
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

    def __fetch_n_dump(self, tablelist=None, exclude=False, dump=False, refetch=True, dirname=None):

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

    def archive(self, path, filename):
        old_dir = os.getcwd()
        tar = tarfile.open(filename, 'w:gz')
        os.chdir(tempfile.tempdir)
        tar.add(os.path.basename(path))
        tar.close()
        os.chdir(old_dir)

    def unarchive(self, filename, path):
        tar = tarfile.open(filename, 'r:gz')
        tar.extractall(path)
        tar.close()
        return path

    def dump(self, filename=None, tablelist=None, exclude=False, refetch=True):
        dirname = tempfile.mkdtemp(dir=tempfile.tempdir)
        self.__fetch_n_dump(tablelist, exclude, dump=True, refetch=refetch, dirname=dirname)
        self.archive(dirname, filename)
        shutil.rmtree(dirname)

    def dumpsql(self, filename=None, tablelist=None, exclude=False, refetch=True):

        if refetch:
            self.fetch(tablelist, exclude)

        with open(filename, 'wb') as f:
            f.write(SQL_ST_PRE.encode())

            for tablename in sorted(self._tables):
                table = self._tables[tablename]

                f.write(SQL_DROP_ST_PRE.format(tablename).encode('utf-8'))

                f.write((u"DROP TABLE IF EXISTS `{0}`;".format(tablename)).encode('utf-8'))

                f.write(SQL_CREATE_ST_PRE.encode('utf-8'))

                f.write(unicode(table).encode('utf-8'))

                f.write(u";\n".encode('utf-8'))

                f.write(SQL_INSERT_ST_PRE.format(tablename).encode('utf-8'))

                for row in table:
                    f.write(row.encode('utf-8'))
                    f.write(u"\n".encode('utf-8'))

                f.write(SQL_INSERT_ST_POST.format(tablename).encode('utf-8'))

            f.write(SQL_ST_POST.format(time.strftime("%Y-%m-%d %H:%M:%S")).encode('utf-8'))

    def restore(self, filename=None, tablelist=None, exclude=False):

        path = tempfile.mkdtemp(dir=tempfile.tempdir)
        print self.unarchive(filename, path)

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
            table = table.load(os.path.join(dirname, tname) + ".obj")

            self._tables[tname] = table
            cur = self._conn.cursor()

            try:
                cur.execute(SQL_ST_PRE)
                cur.execute(u"DROP TABLE IF EXISTS `{0}`;".format(tname))
            except pymysql.err.MySQLError as e:
                print "Unable to DROP table `{0} {1}` exception raised".format(tname, e)
                continue
            try:
                cur.execute(SQL_CREATE_ST_PRE)
                cur.execute(str(table))
            except pymysql.err.MySQLError as e:
                print "Unable to CREATE table `{0} {1}` exception raised".format(tname, e)
                continue

            # restore each row
            if SQL_INSERT_ST_PRE:
                cur.execute(SQL_INSERT_ST_PRE.format(tname))

            for row in table:
                try:
                    cur.execute(row.encode('utf-8'))
                except pymysql.err.MySQLError as e:
                    print "Unable to INSERT INTO table `{0} {1}` exception raised".format(tname, e)
                    print "-" * 10
                    print row
                    print "-" * 10
                    continue

            if SQL_INSERT_ST_POST:
                cur.execute(SQL_INSERT_ST_POST.format(tname))

            # _ = cur.fetchall()

        shutil.rmtree(path)
        return len(self._tables), tablelist


class Table:

    def __init__(self, tablename="", conn=None, charset='utf-8'):
        if not conn or not isinstance(conn, pymysql.connections.Connection):
            return

        self._charset = charset
        self._tablename = tablename
        self._desc = self._get_desc(conn)
        self._ddl = self._get_ddl(conn)
        self._rows = list()
        self._pos = 0

        for row in self._get_rows(conn):
            self._rows.append(row)

    def __str__(self):
        return self._ddl[u'Create Table']

    def __unicode__(self):
        return unicode(self._ddl[u'Create Table']).encode(self._charset)

    def __getitem__(self, item):
        if item < len(self._rows):
            return self._get_row_ddl(self._rows[item])

    def __len__(self):
        return len(self._rows)

    def __iter__(self):
        class TableIterator:
            def __init__(self, table):
                self._pos = 0
                self._table = table

            def __iter__(self):
                return self

            def next(self):
                try:
                    i = self._table._get_row_ddl(self._table._rows[self._pos])
                    self._pos += 1
                except:
                    raise StopIteration()
                return i

            __next__ = next

        return TableIterator(table=self)

    # def next(self):
    #     if self._pos < len(self._rows):
    #         i = self._get_row_ddl(self._rows[self._pos])
    #         self._pos += 1
    #         return i
    #     else:
    #         raise StopIteration()
    #
    # __next__ = next

    def rows(self):
        return self._rows[:]

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

    def _get_rows(self, conn):
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

    def _get_row_ddl(self, row):
        query = ""
        fields = self._desc
        query += "INSERT INTO `" + self._tablename + "` ( `" + "`,`".join(fields.keys()) + "` ) VALUES ("

        first = True
        for field in fields.keys():
            query += "," if first is False else ""
            first = False

            if fields[field] in ("blob", "longblob", "mediumblob"):
                query += "UNHEX('" + row[field] + "')"
            elif fields[field] in "date":
                query += pymysql.converters.escape_date(row[field])
            else:
                val = row[field]
                if isinstance(val, types.NoneType):
                    query += pymysql.converters.escape_None(val)
                elif isinstance(val, (int, long)):
                    query += pymysql.converters.escape_int(val)
                elif isinstance(val, float):
                    query += pymysql.converters.escape_float(val)
                elif isinstance(val, bool):
                    query += pymysql.converters.escape_bool(val)
                elif isinstance(val, datetime.datetime):
                    query += "'" + str(val) + "'"
                elif isinstance(val, types.UnicodeType):
                    query += pymysql.converters.escape_unicode(val)

        query += ");"

        # if int(VERBOSE) >= 4:
        #     # noinspection PyBroadException
        #     try:
        #         print u"DEBUG:\n {0}".format(query).encode(self._charset, errors='ignore')
        #     except:
        #         pass
        return query

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
            print u"ERROR: Unable to save object to file [{0}] : {1}".format(filename, repr(e))

    def load(self, filename):
        """
        Load a table from a file using python pickle
        :param filename: target filename
        :return: table object
        """
        try:
            f = open(filename, "rb")
            self = pickle.load(f)
            self._pos = 0
            f.close()
        except Exception as e:
            print u"ERROR: Unable to load object from file: [{0}] : {1}".format(filename, repr(e))
            sys.exit(20)
        return self


def convert_bit(b):
    b = "\x00" * (8 - len(b)) + b  # pad w/ zeroes
    return struct.unpack(">Q", b)[0]


if sys.version_info > (2, 7):
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

        path = args.target

        if args.restore:
            db.restore(filename=path, tablelist=args.tables, exclude=args.exclude)
            return
        if args.dump:
            db.dump(filename=path, tablelist=args.tables, exclude=args.exclude)


def main():

    if sys.version_info > (2, 7):
        if len(sys.argv) > 1:
            _parse_command(_build_parser())
            return

    fields = {
        "action": {
            "default": "dump",
            "choices": ["dump", "restore", "dumpsql"],
            "type": "str"
        },
        "db": {"required": True, "type": "str"},
        "host": {"required": False, "default": "localhost", "type": "str"},
        "user": {"required": True, "type": "str"},
        "path": {"required": True, "type": "str"},
        "port": {"required": False, "default": 3306, "type": "int"},
        "password": {"required": True, "type": "str"},
        "charset": {"required": False, "default": "utf8", "type": "str"},
        "exclude": {"required": False, "default": False, "type": "bool"},
        "tables": {"required": False, "default": None, "type": "list"}
    }

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

    path = args['path']

    try:
        if "restore" in args['action']:
            db.restore(filename=path, tablelist=args['tables'], exclude=args['exclude'])
        if "dump" in args['action']:
            db.dump(filename=path, tablelist=args['tables'], exclude=args['exclude'])
        if "dumpsql" in args['action']:
            db.dumpsql(filename=path, tablelist=args['tables'], exclude=args['exclude'])
    except pymysql.err.MySQLError as e:
        m.fail_json(msg=str(e))

    m.exit_json(changed=True, meta=m.params)


if __name__ == "__main__":
    main()
