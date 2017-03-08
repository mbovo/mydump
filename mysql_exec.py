#!/usr/bin/env python2
# -*- coding: UTF-8 -*-

import sys
import pymysql
from ansible.module_utils.basic import *


"""
BLa
"""

__author__ = "Manuel Bovo <mbovo@facilitylive.com>"
__license__ = "MIT"
__version__ = "1.0.0"

VERBOSE = 0


def myexec(conn=None,query=None):
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()


def convert_bit(b):
    b = "\x00" * (8 - len(b)) + b # pad w/ zeroes
    return struct.unpack(">Q", b)[0]


def main():

    fields = {
        "db": {"required": True, "type": "str"},
        "host": {"required": False, "default": "localhost", "type": "str"},
        "user": {"required": True, "type": "str"},
        "port": {"required": False, "default": 3306, "type": "int"},
        "password": {"required": True, "type": "str"},
        "charset": {"required": False, "default": "utf8", "type": "str"},
        "query": {"required": True, "type": "str"}
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
    except pymysql.err.MySQLError as e:
        m.fail_json(msg=str(e))

    query = args['query']

    try:
        res = myexec(conn, query)
    except pymysql.err.MySQLError as e:
        m.fail_json(msg=str(e))

    m.exit_json(changed=True, results=res)


if __name__ == "__main__":
    main()
