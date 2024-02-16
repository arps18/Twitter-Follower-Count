package wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.LogManager

object RsRMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nrs.RSR <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RS R")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val XtoY =
      textFile.map(line => {
          line.split(",")
        }).filter(users => users(0).toInt < 40000 && users(1).toInt < 40000)
        .map(users => (users(0), users(1)))


    val YtoZ = XtoY.map {
      case (user1, user2) => (user2, user1)
    }

    val pathLength2 = XtoY.join(YtoZ).filter(joinedRDD => {
      val WV = joinedRDD._2
      WV._1 != WV._2
    }).map {
      case (userY, (userZ, userX)) => ((userZ, userX), userY)
    }

    val ZtoX = XtoY.map {
      case (user1, user2) => ((user1, user2), "")
    }


    val socialTriangle = pathLength2.join(ZtoX)

    println("Social Triangle Count: "+ socialTriangle.count()/3)

  }
}