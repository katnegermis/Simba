/*
 *  Copyright 2016 by Simba Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

// scalastyle:off println
package edu.utah.cs.simba.examples

import edu.utah.cs.simba.SimbaContext
import edu.utah.cs.simba.index.RTreeType
import edu.utah.cs.simba.spatial.Point
// import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

// import org.locationtech.spatial4j.distance.GeodesicSphereDistCalc.Vincenty
// import org.locationtech.spatial4j.shape.impl.PointImpl
// import org.locationtech.spatial4j.shape.Point
// import org.locationtech.spatial4j.context.SpatialContext

import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType};



/**
  * Created by dong on 1/21/16.
  */
object SQL {

  // case class Airport(airport_id: String, pointwg84: (Double, Double), lat: Double, lon: Double)
  case class Airport(id: Integer, lat: Double, lon: Double)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("glossy_scala").setMaster("spark://127.0.0.1:7077")
    val sc = new SparkContext(sparkConf)
    val simbaContext = new SimbaContext(sc)
    // val sqlContext = new SQLContext(sc)


    import simbaContext.implicits._
    import simbaContext.SimbaImplicits._

    // import sqlContext.implicits._

    val d = sc.textFile("/home/katnegermis/thesis/simba/data/openflights_airports.csv").map(_.split(",")).map(p => Airport(p(0).toInt, p(1).toDouble, p(2).toDouble)).toDF

    // import org.apache.spark.sql.functions._

    // val left = simbaContext.read.json(rdd).toDF()
    // left.printSchema

    val left = d.toDF("id", "lat", "lon")
    val right = d.toDF("id2", "lat2", "lon2")
    left.printSchema
    right.printSchema

    left.distanceJoin(right,
          Array("lat", "lon"), // left key string array
          Array("lat2", "lon2"), // right key string array
          10.0).toDF().collect().foreach(println)

    // simbaContext.sql("""
    // SELECT *
    // FROM airports
    // LIMIT 5
    // """).toDF().collect().foreach(println)

    // val d = new Vincenty()
    // simbaContext.udf.register("glossyDistance", (p1: (Double, Double), p2: (Double, Double)) => d.distance(new PointImpl(p1._1, p1._2, SpatialContext.GEO), p2._1, p2._2))


    // 2016-12-27: Not supported in standalone version
    // simbaContext.sql("""
    // SELECT *
    // FROM airports AS l JOIN airports AS r ON POINT(r.lat, r.lon)
    // IN CIRCLERANGE(POINT(l.lat, l.lon), 500)
    // LIMIT 10
    // """).toDF().collect().foreach(println)

    sc.stop()
    println("Finished.")
  }
}
