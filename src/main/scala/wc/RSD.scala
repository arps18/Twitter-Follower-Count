package wc

import org.apache.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

class RSDMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nrs.RSDMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RS-D")
    conf.set("spark.sql.join.preferSortMergeJoin", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val textFile = sc.textFile(args(0))


    val XtoY =
      textFile.map(line => {
          line.split(",")
        }).filter(users => users(0).toInt < 10000 && users(1).toInt < 10000)
        .map(users => Row(users(0), users(1)))


    val schema = new StructType()
      .add(StructField("userIdX", StringType, true))
      .add(StructField("userIdY", StringType, true))


    val df = sqlContext.createDataFrame(XtoY, schema);

    val xToY = df.select('userIdX as "df1_X", 'userIdY as "df1_Y").as("XtoY")

    val yToZ = df.select('userIdX as "df2_X", 'userIdY as "df2_Y").as("YtoZ")

    val pathLength2 = xToY.join(yToZ)
      .where($"XtoY.df1_Y" === $"YtoZ.df2_X" && $"XtoY.df1_X" =!= $"YtoZ.df2_Y")

    val socialTriangle = pathLength2.as("Path")
      .join(df.as("ZtoX"))
      .where($"Path.df2_Y" === $"ZtoX.userIdX" && $"Path.df1_X" === $"ZtoX.userIdY")

    println("Social Triangle Count " + socialTriangle.count()/3)
    println(socialTriangle.queryExecution.executedPlan)
  }
}