#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import collections
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from mysql import SQLDB

def get_region_data_tbl(region):
    return region + "_data"

def get_region_change_tbl(region):
    return region + "_change"

class Apartment(object):
    def __init__(self, aid, price, nts, uts):
        self.aid = aid
        self.price = price
        self.nts = nts
        self.uts = uts

class PriceChange(object):
    def __init__(self, aid, old_price, new_price, ts):
        self.aid = aid
        self.old_price = old_price
        self.new_price = new_price
        self.ts = ts

    def __str__(self):
        s = "Price change: "
        s += "{}/{}/{}/{}".format(self.aid, self.old_price,
                                  self.new_price, self.ts)
        return s

class DayPrice(object):
    def __init__(self, avg_price, mid_price, apartment_count, ts):
        self.avg_price = avg_price
        self.mid_price = mid_price
        self.apartment_count = apartment_count
        self.ts = ts

    def csv(self):
        s = ""
        s += "{}/{},".format(self.ts.month, self.ts.day)
        s += "{},".format(self.avg_price)
        s += "{},".format(self.mid_price)
        s += "{}\n".format(self.apartment_count)
        return s

    def __str__(self):
        s = ""
        s += "{}/{}/{}/{}".format(self.avg_price, self.mid_price,
                                  self.apartment_count, self.ts)
        return s

class ApartmentsData(object):
    def __init__(self, location, region):
        self.location = location
        self.region = region
        self.read_apartment_data()
        self.read_price_change()
        self.collect_first_price()

    def has_apartment(self):
        if len(self.data_nts) == 0:
            return False
        else:
            return True

    def get_first_day(self):
        return self.data_nts[0].nts

    def get_last_day(self):
        return self.data_uts[-1].uts

    def read_apartment_data(self):
        self.data_nts = self.read_db_apartment_data()
        self.data_uts = sorted(self.data_nts, key=lambda x:x.uts)
        self.data_uts = collections.deque(self.data_uts)

    def read_price_change(self):
        self.price_changes = self.read_db_price_change() 

    def collect_first_price(self):
        self.first_price = {}

        for price_change in self.price_changes:
            if price_change.aid not in self.first_price:
                self.first_price[price_change.aid] = price_change.old_price

        for apartment in self.data_nts:
            if apartment.aid not in self.first_price:
                self.first_price[apartment.aid] = apartment.price

    def get_sql_loc(self):
        if self.location == "":
            loc = "LIKE '%'"
        else:
            loc = "= '{0}'".format(self.location)
        return loc
    
    def read_db_apartment_data(self):
        db = SQLDB()
        data_tbl = get_region_data_tbl(region)
        loc = self.get_sql_loc()
        sql_cmd = "SELECT aid, price, "\
                  "       DATE(FROM_UNIXTIME(nts)), "\
                  "       DATE(FROM_UNIXTIME(uts)) "\
                  "FROM {tbl} WHERE location {loc} ORDER BY nts".\
                  format(tbl=data_tbl, loc=loc)
        apartments = db.select(sql_cmd)
        db.close()

        apartments_nts = collections.deque()
        if apartments is None:
            return apartments_nts

        for apartment_data in apartments:
            apartment = Apartment(*apartment_data)
            apartments_nts.append(apartment)
        return apartments_nts

    def read_db_price_change(self):
        db = SQLDB()
        data_tbl = get_region_data_tbl(region)
        change_tbl = get_region_change_tbl(region)
        loc = self.get_sql_loc()
        sql_cmd = '''
            SELECT
            t1.aid, old_price, new_price, DATE(FROM_UNIXTIME(ts))
            FROM
            (SELECT aid FROM {data_tbl} WHERE location {loc}) AS t1
            JOIN
            (SELECT aid, old_price, new_price, ts FROM {change_tbl}
             WHERE old_price > 2000) AS t2
            ON t1.aid = t2.aid
            ORDER BY ts;'''.format(data_tbl=data_tbl,
                                   loc=loc,
                                   change_tbl=change_tbl)
        changes = db.select(sql_cmd)
        db.close()
        if changes is None:
            return []

        price_changes = collections.deque()
        for change in changes:
            price_changes.append(PriceChange(*change))
        return price_changes

class LoDayPrices(object):
    def __init__(self, location, region):
        self.apartments_data = ApartmentsData(location, region)
        if not self.apartments_data.has_apartment():
            self.day_prices = []
            return

        self.location = location
        self.region = region
        self.day_prices = self.calc_day_prices()
        del self.apartments_data

    def calc_day_prices(self):
        apartments_data = self.apartments_data
        today = apartments_data.get_first_day()
        last_day = apartments_data.get_last_day()
        one_day = datetime.timedelta(days=1)
        today_apartments = {}

        day_prices = []
        while today <= last_day:
            self.collect_today_apartments(today, today_apartments)
            self.apply_appartment_price_change(today, today_apartments)
            day_price = self.calc_today_price(today, today_apartments)
            day_prices.append(day_price)
            self.remove_sold_apartments(today, today_apartments)
            today += one_day

        return day_prices

    def collect_today_apartments(self, today, today_apartments):
        apartments_data = self.apartments_data
        data_nts = apartments_data.data_nts
        while len(data_nts) > 0 and  data_nts[0].nts == today:
            apartment = data_nts.popleft()
            today_apartments[apartment.aid] = apartment
            apartment.price = apartments_data.first_price[apartment.aid]

    def apply_appartment_price_change(self, today, today_apartments):
        apartments_data = self.apartments_data
        price_changes = apartments_data.price_changes
        while len(price_changes) > 0 and price_changes[0].ts == today:
            price_change = price_changes.popleft()
            if price_change.aid not in today_apartments:
                raise Exception("Apartment not found for {}".format(price_changes))
            apartment = today_apartments[price_change.aid]
            apartment.price = price_change.new_price

    def get_mid_price(self, a):
        mid = len(a)/2
        if len(a) == 0:
            return 0
        elif len(a) % 2 == 0:
            return (a[mid-1] + a[mid])/2
        else:
            return a[mid]

    def calc_today_price(self, today, today_apartments):
        prices = []
        for aid, apartment in today_apartments.items():
            prices.append(apartment.price)

        if len(prices) == 0:
            raise Exception("No apartments on {}".format(today))

        prices.sort()
        avg_price = sum(prices)/len(prices)
        mid_price = self.get_mid_price(prices)
        today_price = DayPrice(avg_price, mid_price, len(prices), today)
        return today_price

    def remove_sold_apartments(self, today, today_apartments):
        apartments_data = self.apartments_data
        data_uts = apartments_data.data_uts
        while len(data_uts) > 0 and  data_uts[0].uts == today:
            apartment = data_uts.popleft()
            today_apartments.pop(apartment.aid)

    def get_dates_list(self):
        day_prices = self.day_prices
        dates = [day_prices[i].ts for i in xrange(len(day_prices))]
        return dates

    def get_avg_prices(self):
        day_prices = self.day_prices
        avg_prices = [day_prices[i].avg_price for i in xrange(len(day_prices))]
        return avg_prices

    def get_mid_prices(self):
        day_prices = self.day_prices
        mid_prices = [day_prices[i].mid_price for i in xrange(len(day_prices))]
        return mid_prices

    def get_count_list(self):
        day_prices = self.day_prices
        s = [day_prices[i].apartment_count for i in xrange(len(day_prices))]
        return s

    def __str__(self):
        result = ""
        for day_price in self.day_prices:
            result += "{}\n".format(day_price)
        return result

reload(sys)
sys.setdefaultencoding("utf-8")

location = "未名园"
#location = ""
region = "meilong"
#region = "jiangninglu"
day_prices1 = LoDayPrices(location, region)

sns.set_style("darkgrid")
fig = plt.figure()
ax1 = fig.add_subplot(111)
ax1.plot(day_prices1.get_dates_list(), day_prices1.get_avg_prices(), color="r")
ax2 = ax1.twinx()
ax2.plot(day_prices1.get_dates_list(), day_prices1.get_count_list(), color="b")
ax2.set_yticks(np.linspace(ax2.get_yticks()[0],
               ax2.get_yticks()[-1],
               len(ax1.get_yticks())))
plt.show()
