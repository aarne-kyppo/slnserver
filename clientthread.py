#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'aarnek'
from threading import Thread
import socket
import psycopg2 as ps
from haversine import haversine

logfile = "/var/www/log.txt"


class ClientThread(Thread):
    def __init__(self, client, server):
        super(ClientThread, self).__init__()
        self.client = client
        self.server = server
        self.coords = []
        self.conn = ps.connect('dbname=testi user=postgres password=bRewa5UD')
        self.cur = self.conn.cursor()
        self.dist = 2000
        self.multiplier = 2
        self.mindistance = 400


    def __del__(self):
        print 'object deleted'

    def run(self):
        try:
            fp = open(logfile, 'a+')
            fp.write("startsession\n")
            fp.write("===========\n")
            prevloc = None
            count = 0
            while 1:
                msg = self.client.recv(1024)
                if msg:
                    if msg is "endService":
                        raise Exception
                    locarr = msg[:-1].split(";")
                    loc = (float(locarr[1]), float(locarr[0]))

                    self.coords.append(loc)
                    if prevloc:
                        adist = haversine(prevloc, loc) * 1000
                        if adist < self.mindistance:
                            adist = self.mindistance

                        userslocationsql = "ST_Transform(ST_GeometryFromText('POINT({} {})',4326),2393)".format(
                            loc[0], loc[1])
                        query = "select name from store where ST_Distance({},ST_Transform(geom,2393)) <= {} ORDER BY ST_Distance({},ST_Transform(geom,2393)) <= {}".format(
                            userslocationsql, adist * self.multiplier, userslocationsql, adist * self.multiplier)
                        self.cur.execute(query)
                        closest = "select name, ST_Distance({},ST_Transform(geom,2393)) from store ORDER BY ST_Distance({},ST_Transform(geom,2393)) ASC LIMIT 1".format(
                            userslocationsql, userslocationsql)
                        stores_str = "{} ".format(count)
                        stores = []
                        for store in self.cur.fetchall():
                            stores.append(store[0])
                            stores_str += "{},".format(store[0])
                        if len(stores) > 0:
                            self.client.sendall(stores[0])
                        fp.write(stores_str)
                        fp.write("\n")
                        fp.write("Olet liikkunut {} m\n".format(adist))
                        self.cur.execute(closest)
                        i = self.cur.fetchall()
                        fp.write("{}: {} m\n".format(i[0][0], i[0][1]))
                    prevloc = loc
                    count += 1
        except Exception as e:
            fp.write(e.message)
        finally:
            wkt = "MULTILINESTRING(("
            for coord in self.coords:
                wkt += "{} {},".format(coord[0], coord[1])
            wkt = wkt[:-1]
            wkt += "))\n"
            fp.write(wkt)
            fp.write("==========")
            fp.write("endsession\n\n")
            fp.close()
            self.cur.close()