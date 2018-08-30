#!/usr/bin/env python
#coding=utf-8

import sys
import json
import logging
import MySQLdb
import unicodedata

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

def get_data(query, header, idx, topn):
    db = SQLDB()
    tbls = get_data_tables(db)
    valset = [None for i in xrange(len(tbls))]
    for i, tbl in enumerate(tbls):
        valset[i] = db.select(query.format(tbl=tbl))
    db.close()
    res = reduceValset(valset, idx, topn)
    show_result(res, header)

def get_data_tables(db):
    res = db.select("SHOW TABLES")
    tbls = []
    for val in res:
        name = val[0]
        if name.endswith("_data"):
            tbls.append(name)
    return tbls

def reduceValset(valset, idx, topn):
    if topn > 0:
        getmax = True
    else:
        getmax = False
        topn = -topn
    res = []
    offsets = [0 for i in xrange(len(valset))]
    for i in xrange(topn):
        topidx = -1
        if getmax:
            topval = -sys.maxint
        else:
            topval = sys.maxint
        for j in xrange(len(valset)):
            k = offsets[j]
            if valset[j] is None or k >= len(valset[j]):
                continue
            val = valset[j][k]
            if getmax and val[idx] > topval:
                    topidx, topval = j, val[idx]
            if not getmax and val[idx] < topval:
                    topidx, topval = j, val[idx]
        if topidx == -1:
            break
        k = offsets[topidx]
        res.append(valset[topidx][k])
        offsets[topidx] += 1
    return res

def show_result(res, header):
    lines = []
    for val in res:
        lines.append([str(x) for x in val])
    tbl = PrettyTable(header, lines)
    tbl.show()

widths = [
    (126,    1), (159,    0), (687,     1), (710,   0), (711,   1), 
    (727,    0), (733,    1), (879,     0), (1154,  1), (1161,  0), 
    (4347,   1), (4447,   2), (7467,    1), (7521,  0), (8369,  1), 
    (8426,   0), (9000,   1), (9002,    2), (11021, 1), (12350, 2), 
    (12351,  1), (12438,  2), (12442,   0), (19893, 2), (19967, 1),
    (55203,  2), (63743,  1), (64106,   2), (65039, 1), (65059, 0),
    (65131,  2), (65279,  1), (65376,   2), (65500, 1), (65510, 2),
    (120831, 1), (262141, 2), (1114109, 1),
]
 
def get_char_width(o):
    """Return the screen column width for unicode ordinal o."""
    global widths
    if o == 0xe or o == 0xf:
        return 0
    for num, wid in widths:
        if o <= num:
            return wid
    return 1

def get_width(s):
    width = 0
    try:
        s.decode("ascii")
    except:
        pass
    else:
        return len(s)
    for o in s.decode("utf-8"):
        width += get_char_width(o)
    return width*2

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
                col_limit[i] = max(col_limit[i], get_width(line[i]))
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
            s += (" " * (self.col_limit[i]-get_width(line[i])))
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

# +----------+-------------+------+-----+---------+-------+
# | Field    | Type        | Null | Key | Default | Extra |
# +----------+-------------+------+-----+---------+-------+
# | location | varchar(64) | YES  |     | NULL    |       |
# | aid      | varchar(32) | NO   | PRI | NULL    |       |
# | price    | int(11)     | YES  |     | NULL    |       |
# | size     | varchar(32) | YES  |     | NULL    |       |
# | total    | int(11)     | YES  |     | NULL    |       |
# | nts      | bigint(20)  | YES  |     | NULL    |       |
# | uts      | bigint(20)  | YES  |     | NULL    |       |
# | subway   | int(11)     | YES  |     | NULL    |       |
# | station  | varchar(16) | YES  |     | NULL    |       |
# | smeter   | int(11)     | YES  |     | NULL    |       |
# | floor    | varchar(4)  | YES  |     | NULL    |       |
# | tfloor   | int(11)     | YES  |     | NULL    |       |
# | year     | int(11)     | YES  |     | NULL    |       |
# | withlift | varchar(4)  | YES  |     | NULL    |       |
# | visitcnt | int(11)     | YES  |     | NULL    |       |
# +----------+-------------+------+-----+---------+-------+

def total1():
    query = "SELECT location, aid, price, total FROM {tbl} " + \
            "WHERE DAYOFMONTH(FROM_UNIXTIME(uts)) = DAYOFMONTH(NOW()) "+ \
            "ORDER BY total DESC " + \
            "LIMIT 10"
    header = ["location", "aid", "price", "total"]
    idx = 3
    topn = 10
    get_data(query, header, idx, topn)

def total2():
    query = "SELECT location, aid, price, total FROM {tbl} " + \
            "WHERE DAYOFMONTH(FROM_UNIXTIME(uts)) = DAYOFMONTH(NOW()) "+ \
            "ORDER BY total " + \
            "LIMIT 10"
    header = ["location", "aid", "price", "total"]
    idx = 3
    topn = -10
    get_data(query, header, idx, topn)

def tfloor():
    query = "SELECT location, aid, price, total, tfloor FROM {tbl} " + \
            "WHERE DAYOFMONTH(FROM_UNIXTIME(uts)) = DAYOFMONTH(NOW()) "+ \
            "ORDER BY total " + \
            "LIMIT 10"
    header = ["location", "aid", "price", "total", "tfloor"]
    idx = 4
    topn = 10
    get_data(query, header, idx, topn)

def price1():
    query = "SELECT location, aid, price, total FROM {tbl} " + \
            "WHERE DAYOFMONTH(FROM_UNIXTIME(uts)) = DAYOFMONTH(NOW())-1 "+ \
            "ORDER BY price DESC " + \
            "LIMIT 10"
    header = ["location", "aid", "price", "total"]
    idx = 2
    topn = 10
    get_data(query, header, idx, topn)

def price2():
    query = "SELECT location, aid, price, total FROM {tbl} " + \
            "WHERE DAYOFMONTH(FROM_UNIXTIME(uts)) = DAYOFMONTH(NOW()) "+ \
            "ORDER BY price " + \
            "LIMIT 10"
    header = ["location", "aid", "price", "total"]
    idx = 2
    topn = -10
    get_data(query, header, idx, topn)

def main():
    price1()

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding("utf-8")
    main()

