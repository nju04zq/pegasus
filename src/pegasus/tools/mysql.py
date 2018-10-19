#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import logging

MySQL_conf = {
        "host": "127.0.0.1",
        "user": "root",
        "passwd": "root",
        "db": "lianjia_pegasus",
        "local_infile": 1,
        "charset": "utf8mb4"
}

class SQLDB(object):
    def __init__(self):
        self.db_name = MySQL_conf["db"]
        self.db = MySQLdb.connect(**MySQL_conf)
        self.cursor = self.db.cursor()

    def execute(self, cmd):
        try:
            self.cursor.execute(cmd)
            self.db.commit()
        except:
            logging.error("Fail to execute {}".format(cmd))
            self.db.rollback()
            raise

    def insert(self, cmd):
        self.execute(cmd)

    def update(self, cmd):
        self.execute(cmd)

    def select(self, cmd):
        try:
            self.cursor.execute(cmd)
            if self.cursor.rowcount == 0:
                return []
            else:
                return self.cursor.fetchall()
        except:
            logging.error("Fail to execute {}".format(cmd))
            raise

    def close(self):
        self.db.close()

