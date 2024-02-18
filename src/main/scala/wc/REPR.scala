package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object RepRMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nrep.RepRMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Replicated join")
    val sc = new SparkContext(conf)
    val accum = sc.longAccumulator;
    val textFile = sc.textFile(args(0))

    val XtoY =
      textFile.map(line => {
          line.split(",")
        }).filter(users => users(0).toInt < 10000 && users(1).toInt < 10000)
        .map(users => (users(0).toInt, users(1).toInt))

    val userMap = XtoY.map(rdd => (rdd._1, Set(rdd._2)))
      .reduceByKey(_ ++ _)

    val broadcastRdd = sc.broadcast(userMap.collect.toMap)


    val socialTriangleCount = XtoY.map {
      case (userX, userY) => broadcastRdd.value.getOrElse(userY, Set[Int]()).foreach {
        userZ => if(userZ != userX && broadcastRdd.value.getOrElse(userZ, Set[Int]()).contains(userX)) {
          accum.add(1)
        }
      }
    }

    socialTriangleCount.collect()
    println("Social Triangle Count: " + accum.value/3)
  }
}