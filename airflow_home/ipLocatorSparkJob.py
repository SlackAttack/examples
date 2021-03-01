from pyspark import SparkConf, SparkContext
import os
import sys
sys.path.append('/home/hadoop/libs')

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.addPyFile("libs.zip")

from geopy.distance import distance


def parseIpLookup(line):

    fields = line.replace("\"", "").split(',')

    try:
        range = (int(fields[0]), int(fields[1]))
    except:
        range = (None, None)

    try:
        coords = (float(fields[6]), float(fields[7]))
    except:
        coords = (None, None)

    return (range, coords)


def convertDottedIp(line):

    fields = line.replace("\"", "").split(',')

    dots = list(map(int, fields[1].split(".")))
    red = dots[0]
    for d in range(0, len(dots)-1):
        a = red<<8 | dots[d+1]
        red = a
    return (red, int(fields[0]))

def appendCoords(event):

    eventId = event[0][1]
    lat = event[1][1][0]
    lng = event[1][1][1]

    return (eventId, (lat, lng))


def parseDistricts(line):

    fields = line.split(',')

    name = fields[0]
    coords = (fields[1], fields[2])
    radius = fields[3]

    return (name, (coords, radius))

def appendDistrict(event):

    eventCoords = event[1]

    district = None
    for k,v in districtLookup.items():

        midpoint = (float(v[0][0]), float(v[0][1]))
        if distance(midpoint, eventCoords).miles < int(v[1]):

            district = k

    return (event[0], eventCoords[0], eventCoords[1], district)

def toCSVLine(line):

  return ','.join(str(d) for d in line)

ipLookup = sc.textFile('s3n://boatsetter-warehouse/ip_locations.csv')
eventIp = sc.textFile('s3n://boatsetter-warehouse/boat_listing_ips.txt')
districts = sc.textFile('s3n://boatsetter-warehouse/districts.txt')

# lookupRdd is pair of tuples - the range ip range as the key and the coords as the val
lookupRDD = ipLookup.map(parseIpLookup)
lookupRDD = lookupRDD.partitionBy(100)

# districtRDD - k = district name, v = ((coords), radius)
districtsRDD = districts.map(parseDistricts)
districtLookup = districtsRDD.collectAsMap()

# eventRdd has the event_id as the v and the ip address (repr as an int) as the k
eventRDD = eventIp.map(convertDottedIp)
eventsPartitioned = eventRDD.partitionBy(100)
joinedEvents = eventsPartitioned.cartesian(lookupRDD).filter(lambda x: (x[0][0] > x[1][0][0]) and (x[0][0] <x[1][0][1]))
joinedEvents = joinedEvents.map(appendCoords)
joinedEvents  = joinedEvents.map(appendDistrict)
joinedEvents = joinedEvents.map(toCSVLine)

joinedEvents.coalesce(1, shuffle=True).saveAsTextFile('s3n://boatsetter-warehouse/boat_listing_locations')
