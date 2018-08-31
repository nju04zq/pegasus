#!/usr/bin/env python

import os
import shutil
import logging
import MySQLdb
import tarfile
import datetime

MySQL_conf = {
        "host": "localhost",
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

def export_one_table(db, path, tbl_name):
    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    fname = tbl_name+".csv"
    src = os.path.join("/tmp", "pegasus_"+fname+ts)
    dst = os.path.join(path, fname)
    cmd = "SELECT * FROM {tbl} INTO OUTFILE '{fname}' "\
          "FIELDS TERMINATED BY ',' ENCLOSED BY '\"'"\
          "LINES TERMINATED BY '\r\n'".format(tbl=tbl_name, fname=src)
    db.execute(cmd)
    shutil.copyfile(src, dst)
    # can't remove it, permission problem
    #os.remove(src)

def get_tables(db):
    cmd = "SHOW TABLES"
    vals = db.select(cmd)
    tables = []
    for val in vals:
        tables.append(val[0])
    return tables

def export_():
    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    dirname = "data-{0}".format(ts)
    os.mkdir(dirname)
    path = os.path.abspath(dirname)
    db = SQLDB()
    tables = get_tables(db)
    for table in tables:
        export_one_table(db, path, table)
    tgz_fname = path + ".tgz"
    with tarfile.open(tgz_fname, "w:gz") as tf:
        for fname in os.listdir(path):
            tf.add(os.path.join(dirname, fname))
    shutil.rmtree(path)
    db.close()
    print "Data packed to {0}".format(tgz_fname)

def main():
    export_()

if __name__ == "__main__":
    main()
