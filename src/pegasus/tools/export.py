#!/usr/bin/env python

import os
import shutil
import tarfile
import datetime
from mysql import SQLDB

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
        if val[0] != "update_history":
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
