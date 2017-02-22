#!/usr/bin/env python2

import argparse
import base64
import datetime
import os
import pickle
import pprint
import sys
import types

import pymysql

SILENT = False
DEBUG = False

class myBlob:
    value=None

    def __init__(self,s):
        self.value=base64.b64encode(s)

    @property
    def __str__(self):
        return base64.b64decode(self.value)



def debugb(string):
    if SILENT:
        return 0
    sys.stderr.write(string)
    for i in range(0, len(string)):
        sys.stderr.write("\b")


def debug(string):
    if SILENT:
        return 0
    sys.stderr.write(string.encode('utf-8'))


def prepare(dbname, prefix="backup_", usedate=False, fulldir=None):
    if fulldir:
        dirname = fulldir
    else:
        dirname = os.getcwd() + "/" + prefix + dbname
        if usedate:
            now = datetime.datetime.now()
            dest += "_" + now.strftime("%Y-%m-%d_%H-%M")
    try:
        os.mkdir(dirname)
    except OSError as e:
        debug(str(e) + "\n")

    return dirname


def dumptable(filename, obj):
    f = open(filename, "wb")
    pickle.dump(obj, f, -1)
    f.close()


def loadtable(filename):
    f = open(filename, "rb")
    obj = pickle.load(f)
    f.close()
    return obj


def open_db(host='localhost', db='', user='root', password='', charset='utf8'):
    con = pymysql.connect(host=host,
                          user=user,
                          password=password,
                          db=db,
                          charset=charset,
                          cursorclass=pymysql.cursors.DictCursor)
    return con


def get_table_desc(con=None, tablename=""):

    """

    :param con: Connection Object
    :param tablename: String database name
    :return: dict with association Field:Type
    """
    cur = con.cursor()
    cur.execute("DESC `" + tablename + "`;")

    desc = dict()
    for field in cur.fetchall():
        desc[field[u'Field']] = field[u'Type']
    #pprint.pprint(desc)
    return desc


def get_table_ddl(con=None, tablename=""):
    cur = con.cursor()
    cur.execute("SHOW CREATE TABLE `" + tablename + "`;")
    return cur.fetchone()


def get_table_rows(con=None, tablename=""):
    cur = con.cursor()
    cur.execute("SELECT * FROM `" + tablename + "`;")
    return cur.fetchall()


def get_blob_field(con=None, field="", table=""):
    cur = con.cursor()
    cur.execute("SELECT HEX(" + field + ") from `" + table + "`;")
    return cur.fetchall()


def get_tables(con=None, dbname=None, prefix=None, usedate=False, fulldir=None):
    cur = con.cursor()
    dirname = prepare(dbname, prefix, usedate, fulldir)

    cur.execute("SHOW TABLES")
    for table in cur.fetchall():

        # if table.values()[0] not in ("AssetEntry"):
        #     continue
        obj = get_table_ddl(con, table.values()[0])
 #       fields =  get_table_desc(con, table.values()[0] )

        debug(table.values()[0])

        lines = get_table_rows(con, table.values()[0])
        # loop on keys in order to change values

 #       for lk in range( len(lines) ):
 #           for key in lines[lk].keys():
 #               if fields[key] == "blob":
 #                   lines[lk][key] = myBlob(get_blob_field(con, fields[key], table.values()[0]))
 #               pprint.pprint(lines[lk][key])
        if DEBUG:
            pprint.pprint(lines)

        tot = len(lines)
        lines = base64.b64encode(pickle.dumps(lines))

        obj[u'Lines'] = lines

        dumptable(dirname + "/" + table.values()[0] + ".obj", obj)
        debug("\t" + str(tot) + " Rows OK.")
        debug("\n")
    con.close()


def create_table(con=None, obj=None):
    cur = con.cursor()
    tablename = obj[u'Table']
    tablestatement = obj[u'Create Table']

    debug("TABLE\t" + tablename)

    safe = """
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
    cur.execute(safe)

    safe = """
    /*!40101 SET @saved_cs_client     = @@character_set_client */;
    /*!40101 SET character_set_client = utf8 */;
    """
    cur.execute("DROP TABLE IF EXISTS `" + tablename + "`;")
    cur.execute(safe)
    debug("\t DROP")

    cur.execute(tablestatement)
    cur.execute("/*!40101 SET character_set_client = @saved_cs_client */;")
    debug("\t CREATE\t ")

    cur.execute("LOCK TABLES `" + tablename + "` WRITE; /*!40000 ALTER TABLE `" + tablename + "` DISABLE KEYS */;")

    lines = pickle.loads(base64.b64decode(obj[u'Lines']))
    tot = len(lines)
    ncur = 1
    for line in lines:
        query = u""
        query += "INSERT INTO `" + tablename + "` ( " + ",".join(line.keys()) + " ) VALUES ("

        first = True
        for val in line.values():
            if first:
                first = False
            else:
                query += u", "
            if isinstance( val, types.NoneType ):
                query += pymysql.converters.escape_None(val)
            elif isinstance(val, (int, long)):
                query += pymysql.converters.escape_int(val)
            elif isinstance(val, float):
                query += pymysql.converters.escape_float(val)
            elif isinstance(val, bool):
                query += pymysql.converters.escape_bool(val)
            elif isinstance(val, datetime.datetime):
                query += u"'" + unicode(val) + u"'"
            elif isinstance(val, types.UnicodeType):
                query += pymysql.converters.escape_unicode(val)

        query += ");"

        if DEBUG:
            debug(query + u"\n")
        cur.execute(query.encode('utf-8'))

        debugb("\tINSERT \t" + str(ncur) + "/" + str(tot))
        ncur += 1

    cur.execute("/*!40000 ALTER TABLE `" + tablename + "` ENABLE KEYS */; UNLOCK TABLES `" + tablename + "`; ")
    debug("\n")


def list_fs(path, db, password, user, host, charset):
    for dirname, dirnames, filenames in os.walk(path):
        for filename in filenames:
            # print(filename)
            obj = loadtable(os.path.join(dirname, filename))
            con = open_db(host, db, user, password, charset)
            assert (isinstance(con, pymysql.connections.Connection))
            create_table(con, obj)
            con.close()


def _build_parser():
    parser = argparse.ArgumentParser(prog="mydump")

    g1 = parser.add_mutually_exclusive_group(required=True)
    g1.add_argument("-d", "--dump", action="store_true", help="Dump database to disk (default)")
    g1.add_argument("-r", "--restore", action="store_true", help="Restore database from disk")

    parser.add_argument("-H", "--host", default="localhost", help="Database host (default: localhost)")
    parser.add_argument("-c", "--charset", default="utf8", help="Database and output charset (default: UTF8)")
    parser.add_argument("-o", "--dest", default=None, help="Output destination directory (default: $prefix_$dbname)")
    parser.add_argument("-P", "--prefix", default="backup_", help="Prefix of destination directory (default: backup_ )")
    parser.add_argument("-t", "--timestamp", action="store_true",
                        help="Use timestamp in destination directory (default: false )")

    parser.add_argument("-u", "--user", help="Database Username", required=True)
    parser.add_argument("-p", "--password", help="Database Password", required=True)

    parser.add_argument("-q", "--quiet", help="Be Quiet", action="store_true")
    parser.add_argument("-D", "--debug", help="Be Verbose", action="store_true")

    parser.add_argument("dbname", nargs=1, type=str, help="Database name")

    assert isinstance(parser, object)
    return parser


def _parse_command(parser=None):
    global SILENT, DEBUG
    assert isinstance(parser, argparse.ArgumentParser)
    args = parser.parse_args()

    if len(sys.argv) < 1:
        parser.print_help()
        return 1

    args.dbname = args.dbname[0]
    pprint.pprint(args)

    SILENT = args.quiet
    DEBUG = args.debug

    if args.dump:
        get_tables(open_db(db=args.dbname, password=args.password, user=args.user, host=args.host, charset=args.charset)
                   , args.dbname, args.prefix, args.timestamp, args.dest)

    if args.restore:
        list_fs(prepare(args.dbname, args.prefix, args.timestamp, args.dest),
                db=args.dbname, password=args.password, user=args.user, host=args.host, charset=args.charset)

    return 0


if __name__ == "__main__":
    exit(_parse_command(_build_parser()))
