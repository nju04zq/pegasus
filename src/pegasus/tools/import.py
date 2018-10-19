#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import shutil
import tarfile
from mysql import SQLDB

def import_(tgz_fname):
    with tarfile.open(tgz_fname, "r:gz") as tf:
        tf.extractall()
    dirname = tgz_fname.rstrip(".tgz")
    path = os.path.abspath(dirname)
    db = SQLDB()
    res = []
    for fname in os.listdir(path):
        fpath = os.path.join(path, fname)
        import_one_file(db, fpath, res)
    shutil.rmtree(dirname)
    db.close()
    print "Import from {0} done.".format(tgz_fname)
    show_result(res)

def import_one_file(db, fpath, res):
    tbl_name = os.path.basename(fpath).rstrip(".csv")
    reset_table(db, tbl_name)
    cmd = "LOAD DATA LOCAL INFILE '{fpath}' INTO TABLE {tbl_name} "\
          "CHARACTER SET utf8mb4 "\
          "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "\
          "LINES TERMINATED BY '\r\n'".format(fpath=fpath, tbl_name=tbl_name)
    db.execute(cmd)
    db_lines = db.select("SELECT COUNT(*) from {tbl_name}".format(\
                         tbl_name=tbl_name))[0][0]
    with open(fpath, "r") as fp:
        file_lines = len(fp.readlines())
    res.append((tbl_name, file_lines, db_lines))

def reset_table(db, tbl_name):
    create_table(db, tbl_name)
    cmd = "TRUNCATE TABLE {0}".format(tbl_name)
    db.execute(cmd)

def create_table(db, tbl_name):
    toks = tbl_name.split("_")
    suffix = "_".join(toks[1:])
    if suffix == "data":
        cmd = new_tbl_data
    elif suffix == "change":
        cmd = new_tbl_change
    elif suffix == "change_meta":
        cmd = new_tbl_meta
    cmd = cmd.format(tbl_name=tbl_name)
    db.execute(cmd)

new_tbl_data = '''
CREATE TABLE IF NOT EXISTS `{tbl_name}` (
  `location` varchar(64) DEFAULT NULL,
  `aid` varchar(32) NOT NULL,
  `price` int(11) DEFAULT NULL,
  `size` varchar(32) DEFAULT NULL,
  `total` int(11) DEFAULT NULL,
  `nts` bigint(20) DEFAULT NULL,
  `uts` bigint(20) DEFAULT NULL,
  `subway` int(11) DEFAULT NULL,
  `station` varchar(16) DEFAULT NULL,
  `smeter` int(11) DEFAULT NULL,
  `floor` varchar(4) DEFAULT NULL,
  `tfloor` int(11) DEFAULT NULL,
  `year` int(11) DEFAULT NULL,
  `withlift` varchar(4) DEFAULT NULL,
  `visitcnt` int(11) DEFAULT NULL,
  PRIMARY KEY (`aid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
'''

new_tbl_change = '''
CREATE TABLE IF NOT EXISTS `{tbl_name}` (
  `aid` varchar(32) DEFAULT NULL,
  `old_price` int(11) DEFAULT NULL,
  `new_price` int(11) DEFAULT NULL,
  `old_total` int(11) DEFAULT NULL,
  `new_total` int(11) DEFAULT NULL,
  `ts` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
'''

new_tbl_meta = '''
CREATE TABLE IF NOT EXISTS `{tbl_name}` (
  `aid` varchar(32) DEFAULT NULL,
  `item` varchar(16) DEFAULT NULL,
  `old` varchar(64) DEFAULT NULL,
  `new` varchar(64) DEFAULT NULL,
  `ts` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
'''

class PrettyTable(object):
    def __init__(self, header, lines):
        self.header = header
        self.lines = lines
        self.col_limit = self.get_table_col_limit()
        # pad the seperator between columns
        self.col_seperator = "  "

    # print the whole table
    def show(self):
        sys.stdout.write(self.format())

    # format the whole table, return string
    def format(self):
        output = ""
        output += self.format_table_one_line(self.header)
        output += self.format_table_seperator()
        for oneline in self.lines:
            output += self.format_table_one_line(oneline)
        return output

    # calculate the width limit for each column in table
    def get_table_col_limit(self):
        self.lines.append(self.header)
        col_cnt = len(self.header)
        col_limit = [0 for i in xrange(col_cnt)]
        for line in self.lines:
            if len(line) != col_cnt:
                raise Exception("Table line {0} not match header {1}".format(\
                                line, self.header))
            for i in xrange(len(col_limit)):
                col_limit[i] = max(col_limit[i], len(line[i]))
        self.lines.pop()
        return col_limit

    # format one line in the table, each line is defined by a tuple containing
    # column values. If column value string length is less than the column width
    # limit, extra spaces will be padded
    def format_table_one_line(self, line):
        output = ""
        cols = []
        for i in xrange(len(line)):
            s = ""
            s += line[i]
            s += (" " * (self.col_limit[i]-len(line[i])))
            cols.append(s)
        output += (self.col_seperator.join(cols) + "\n")
        return output

    # format the seperator as -------
    def format_table_seperator(self):
        sep_cnt = sum(self.col_limit)
        # count in column seperators, why -1?, 2 columns only have one
        sep_cnt += (len(self.col_limit) - 1)*len(self.col_seperator)
        # one extra sep to make it pretty
        sep_cnt += 1
        return "-" * sep_cnt + "\n"

def show_result(results):
    header = ["Name", "File lines", "DB lines", "OK?"]
    lines = []
    results.sort(key=lambda x:x[0])
    for res in results:
        line = [str(x) for x in res]
        if res[1] == res[2]:
            line.append("---")
        else:
            line.append("???")
        lines.append(line)
    tbl = PrettyTable(header, lines)
    tbl.show()

def main():
    import_(sys.argv[1])

if __name__ == "__main__":
    main()
