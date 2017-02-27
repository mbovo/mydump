#!/usr/bin/env python2

import argparse
import base64
import datetime
import os
import pickle
import sys
import types

import pymysql

VERBOSE = 0


def prepare(dbname, prefix="backup_", usedate=False, fulldir=None):
    if fulldir:
        dirname = fulldir
    else:
        dirname = os.getcwd() + "/" + prefix + dbname
        if usedate:
            now = datetime.datetime.now()
            dirname += "_" + now.strftime("%Y-%m-%d_%H-%M")

    print u"Store location: {}".format(dirname)
    try:
        os.mkdir(dirname)
    except OSError as e:
        print u"ERROR:\tError creating directory on {} : {}".format(dirname, e.strerror)

    return dirname


def dumptable(filename, obj):
    f = open(filename, "wb")
    pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
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
    return desc


def get_table_ddl(con=None, tablename=""):
    cur = con.cursor()
    cur.execute("SHOW CREATE TABLE `" + tablename + "`;")
    return cur.fetchone()


def get_table_rows(con=None, tablename=""):
    fields = get_table_desc(con, tablename)

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

    cur = con.cursor()
    cur.execute(query)

    return cur.fetchall()


def jump_table(tablename=None, tablelist=None, exclude_mode=False):
    assert(isinstance(tablename, (str, unicode)))
    assert(isinstance(tablelist, (list, tuple)))

    if tablelist and isinstance(tablelist, (list, tuple)):
        if exclude_mode:
            if tablename in tablelist:
                # avoid dump of this table,
                if int(VERBOSE) >= 2:
                    print u"DEBUG: Ignoring {}".format(tablename)
                return True
        else:
            if tablename not in tablelist:
                # avoid dump of this table
                if int(VERBOSE) >= 2:
                    print u"DEBUG: Ignoring {}".format(tablename)
                return True
    return False


def get_tables(con=None, dbname=None, prefix=None, usedate=False, fulldir=None, exclude_mode=False, tables=None):
    """

    :param con: Database Connection
    :param dbname: Database name
    :param prefix:  Database prefix for export directory
    :param usedate: Use timestamp in output directory
    :param fulldir: Use a full directory
    :param exclude_mode:   Exclude?
    :param tables:  Tables list
    """
    global VERBOSE

    cur = con.cursor()
    dirname = prepare(dbname, prefix, usedate, fulldir)

    if int(VERBOSE) >= 1:
        print u"DEBUG: Exclude: {}\nDEBUG: Table List: {}".format(exclude_mode, unicode(tables))

    cur.execute("SHOW TABLES")
    for table in cur.fetchall():

        if jump_table(table.values()[0], tables, exclude_mode):
            continue

        obj = get_table_ddl(con, table.values()[0])

        if int(VERBOSE) >= 1:
            print u"DEBUG: Dumping: {}".format(table.values()[0])

        lines = get_table_rows(con, table.values()[0])

        if int(VERBOSE) >= 3:
            count = 1
            tot = len(lines)
            for line in lines:
                print u"DEBUG:\t      Row {} / {}".format(count, tot)
                count += 1
                for field in line:
                    print u"DEBUG:\t\t   Field: {0:30}  Val: {1}".format(field,
                                                                         line[field]).encode('utf-8', errors='ignore')

        # encoding lines in single object
        lines = base64.b64encode(pickle.dumps(lines, pickle.HIGHEST_PROTOCOL))

        obj[u'Lines'] = lines

        dumptable(dirname + "/" + table.values()[0] + ".obj", obj)
    con.close()


def create_table(con=None, obj=None, exclude_mode=False, tablelist=None):
    global VERBOSE

    cur = con.cursor()
    tablename = obj[u'Table']
    tablestatement = obj[u'Create Table']

    if jump_table(tablename, tablelist, exclude_mode):
        return False

    if int(VERBOSE) >= 1:
        print u"DEBUG: Restoring: {}".format(tablename)

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
    cur.execute(tablestatement)
    cur.execute("/*!40101 SET character_set_client = @saved_cs_client */;")

    cur.execute("LOCK TABLES `" + tablename + "` WRITE; /*!40000 ALTER TABLE `" + tablename + "` DISABLE KEYS */;")

    lines = pickle.loads(base64.b64decode(obj[u'Lines']))

    fields = get_table_desc(con, tablename)

    tot = len(lines)
    ncur = 1
    for line in lines:
        query = ""
        query += "INSERT INTO `" + tablename + "` ( `" + "`,`".join(fields.keys()) + "` ) VALUES ("

        if int(VERBOSE) >= 2:
            print u"DEBUG:\t      Row {} / {}".format(ncur, tot)

        first = True
        for field in fields.keys():
            query += "," if first is False else ""
            first = False

            if int(VERBOSE) >= 3:
                print u"DEBUG:\t\t   Field: {0:30}  Val: {1}".format(field, line[field])\
                    .encode('utf-8', errors='ignore')

            if fields[field] in ("blob", "longblob", "mediumblob"):
                query += "UNHEX('" + line[field] + "')"
            elif fields[field] in "date":
                query += pymysql.converters.escape_date(line[field])
            else:
                val = line[field]
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
                print u"DEBUG:\n {}".format(query).encode('utf-8', errors='ignore')
            except:
                pass

        try:
            cur.execute(query.encode('utf-8'))
        except pymysql.err.ProgrammingError as e:
            print u"Error executing query: {}".format(e)
            print u"{}".format(query.encode('utf-8', errors='ignore'))
            con.close()
            exit(1)

        ncur += 1

    cur.execute("/*!40000 ALTER TABLE `" + tablename + "` ENABLE KEYS */; UNLOCK TABLES `" + tablename + "`; ")


def list_fs(path, db, password, user, host, charset, exclude_mode=False, tables=None):
    if int(VERBOSE) >= 1:
        print u"DEBUG: Exclude: {}\nDEBUG: Table List: {}".format(exclude_mode, unicode(tables))
    for dirname, dirnames, filenames in os.walk(path):
        for filename in filenames:
            obj = loadtable(os.path.join(dirname, filename))
            con = open_db(host, db, user, password, charset)
            assert (isinstance(con, pymysql.connections.Connection))
            create_table(con, obj, exclude_mode, tables)
            con.close()


def _build_parser():
    parser = argparse.ArgumentParser(prog="mydump")

    g1 = parser.add_mutually_exclusive_group(required=False)
    g1.add_argument("-d", "--dump", action="store_true", default=True, help="Dump database to disk (default)")
    g1.add_argument("-r", "--restore", action="store_true", default=False, help="Restore database from disk")

    parser.add_argument("-H", "--host", default="localhost", help="Database host (default: localhost)")
    parser.add_argument("-c", "--charset", default="utf8", help="Database and output charset (default: UTF8)")
    parser.add_argument("-o", "--dest", default=None, help="Output destination directory (default: $prefix_$dbname)")
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

    if len(sys.argv) < 1:
        parser.print_help()
        return 1

    # TODO: only one db at time is supported for now
    args.dbname = args.dbname[0]

    exclude_mode = args.exclude

    VERBOSE = args.verbose

    if int(VERBOSE) > 0:
        print u"DEBUG:\tVerbose {}\nDEBUG\tMYDBC URL: {}:{}@:/{}".format(VERBOSE, args.user, args.password, args.host,
                                                                         args.dbname)

    if args.restore:
        list_fs(prepare(args.dbname, args.prefix, args.timestamp, args.dest),
                db=args.dbname, password=args.password, user=args.user, host=args.host, charset=args.charset,
                exclude_mode=exclude_mode, tables=args.tables)
        return 0

    if args.dump:
        get_tables(open_db(db=args.dbname, password=args.password, user=args.user, host=args.host, charset=args.charset)
                   , args.dbname, args.prefix, args.timestamp, args.dest, exclude_mode, args.tables)

    return 0


if __name__ == "__main__":
    exit(_parse_command(_build_parser()))
