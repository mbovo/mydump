#!/usr/bin/env python2
# -*- coding: UTF-8 -*-

import struct
import io
import pymysql
from ansible.module_utils.basic import AnsibleModule

"""
Pure python implementation of mysql utility as ansible module (and standalone
script for python>2.7) This code uses pymysql 3d party library
This module permit free query execution on a mysql database. Supports inline
query and .sql files
"""

__author__ = "Manuel Bovo <mbovo@facilitylive.com>"
__license__ = "MIT"
__version__ = "2.4.3"

VERBOSE = 0


def my_query(conn=None, query=None):
    if not conn:
        return None
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()


def my_exec(conn=None, path=None):

    n = 0
    res = dict()
    content = list()
    cur = conn.cursor()
    with io.open(path, 'rb') as fp:
        for line in fp:
            n += 1
            if not line.startswith('--') and len(line) > 1:
                content.append(line)
            if line.endswith(";\n") and len(line) > 1:
                # query = "".join(content)
                # print query
                # my_query(conn, "".join(content))
                cur.execute("".join(content))
                rev = cur.fetchall()
                if len(rev) > 0:
                    res[n] = rev
                content = list()

    return dict(statements=n, results=res)


def convert_bit(b):

    b = "\x00" * (8 - len(b)) + b  # pad w/ zeroes
    return struct.unpack(">Q", b)[0]


def main():

    fields = {
        "type": {
            "default": "query",
            "choices": ["query", "file"],
            "type": "str"
        },
        "db": {"required": True, "type": "str"},
        "host": {"required": False, "default": "localhost", "type": "str"},
        "user": {"required": True, "type": "str"},
        "port": {"required": False, "default": 3306, "type": "int"},
        "password": {"required": True, "type": "str"},
        "charset": {"required": False, "default": "utf8", "type": "str"},
        "query": {"required": False, "default": None,  "type": "str"},
        "file": {"required": False, "default": None, "type": "str"},
        "force": {"required": False, "default": False, "type:": "bool"}
    }

    m = AnsibleModule(argument_spec=fields)

    convert_matrix = pymysql.converters.conversions
    convert_matrix[pymysql.FIELD_TYPE.BIT] = convert_bit

    args = m.params
    conn = None
    try:
        conn = pymysql.connect(host=args['host'],
                               db=args['db'],
                               user=args['user'],
                               password=args['password'],
                               charset=args['charset'],
                               conv=convert_matrix,
                               cursorclass=pymysql.cursors.DictCursor)

    except pymysql.err.Error as e:
        m.fail_json(msg=str(e))

    try:
        if args['type'] == "query":
            res = my_query(conn, args['query'])
        else:
            res = my_exec(conn, args['file'])
    except pymysql.err.ProgrammingError as e:
        if bool(args['force']):
            res = str(e)
        else:
            m.fail_json(msg=str(e))
    except (pymysql.err.Error, IOError) as e:
        m.fail_json(msg=str(e))
        return

    m.exit_json(changed=True, results=res)


if __name__ == "__main__":
    main()
