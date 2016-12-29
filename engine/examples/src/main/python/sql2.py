#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

from math import radians, cos, sin, asin, sqrt
import os
import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, DoubleType

from ctimer import CTimer


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000 # Radius of earth in meters. Use 3956 for miles
    return c * r

def euclidean(x1, y1, x2, y2):
    return sqrt((x2-x1)**2 + (y2-y1)**2)

if __name__ == "__main__":
    sc = SparkContext(appName="glossy_test", master="spark://127.0.0.1:7077")
    sqlContext = SQLContext(sc)

    # sqlContext.setConf('spark.sql.index.threshold', '2')
    # sqlContext.setConf('spark.eventLog.enabled', 'true')
    sqlContext.setConf('spark.sql.joins.distanceJoin', 'DJSpark')
    datafile = lambda f: "file://" + os.path.join(os.environ['SPARK_HOME'], "../../data/", f)

    path = datafile("openflights_airports.json")
    rdd = sc.textFile(path)

    # Create a DataFrame from the file(s) pointed to by path
    airports = sqlContext.read.json(rdd)

    # Register this DataFrame as a table.
    airports.registerTempTable("airports")


    with CTimer('Indexing', print_result=True):
        sqlContext.sql("CREATE INDEX pidx ON airports(lat, lon) USE rtree")
        sqlContext.sql("CREATE INDEX pidy ON airports(airport_id) USE hashmap")

    sqlContext.registerFunction("haversine", haversine, returnType=DoubleType())
    sqlContext.registerFunction("euclidean", euclidean, returnType=DoubleType())

    # SQL statements can be run by using the sql methods provided by sqlContext
    results = sqlContext.sql("""
    SELECT l.airport_id, r.airport_id, haversine(l.lonwg84, l.latwg84, r.lonwg84, l.latwg84), euclidean(l.lat, l.lon, r.lat, r.lon)
    FROM airports AS l DISTANCE JOIN airports AS r ON POINT(r.lat, r.lon)
    IN CIRCLERANGE(POINT(l.lat, l.lon), 5000)
    WHERE l.airport_id < r.airport_id
    AND haversine(l.lonwg84, l.latwg84, r.lonwg84, l.latwg84) > 2500
    LIMIT 100
    """)

    with CTimer('Spatial query', print_result=True):
        results = results.collect()

    for result in results:
        print("========================\n=================================\n")
        print(result)
        print("========================\n=================================\n")

    sc.stop()
