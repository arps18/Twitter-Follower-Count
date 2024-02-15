import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object FollowerCountMain {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DSet")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val textFile = sc.textFile(args(0))

    // RDD-A
    val addToCount = (count1: Int, count2: Int) => count1 + count2
    val sumPartitionCount = (p1: Int, p2: Int) => p1 + p2

    val followerCountRDDA = textFile.map(line => {
      val users = line.split(",")
      val followedUser = users(1)
      (followedUser, 1)
    }).aggregateByKey(0)(addToCount, sumPartitionCount)

    println(followerCountRDDA.toDebugString)
    followerCountRDDA.saveAsTextFile(args(1) + "/RDD-A")

    // RDD-F
    val followerCountRDDF = textFile.map(line => {
      val users = line.split(",")
      val followedUser = users(1)
      (followedUser, 1)
    }).foldByKey(0)((x,y) => x+y)

    println(followerCountRDDF.toDebugString)
    followerCountRDDF.saveAsTextFile(args(1) + "/RDD-F")

    // RDD-G
    val followerCountRDDG = textFile.map(line => {
      val users = line.split(",")
      val followedUser = users(1)
      (followedUser, 1)
    }).groupByKey().mapValues(noOfFollowers => noOfFollowers.reduce((x,y) => x+y))

    println(followerCountRDDG.toDebugString)
    followerCountRDDG.saveAsTextFile(args(1) + "/RDD-G")

    // RDD-R
    val followerCountRDDR = textFile.map(line => {
      val users = line.split(",")
      val followedUser = users(1)
      (followedUser, 1)
    }).reduceByKey((x,y) => x+y)

    println(followerCountRDDR.toDebugString)
    followerCountRDDR.saveAsTextFile(args(1) + "/RDD-R")

    // Dataset
    val rdd = textFile.map(line => {
      val users = line.split(",")
      val followedUser = users(1)
      (followedUser, 1)
    })

    val sparkSession =  SparkSession.builder().getOrCreate()
    val dataset =  sparkSession.createDataset(rdd)

    val counts = dataset.groupBy("_1").agg(sum($"_2"))
    counts.rdd.saveAsTextFile(args(1) + "/DSet")

    println(counts.explain(true))
  }

}