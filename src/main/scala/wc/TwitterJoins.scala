import org.apache.spark.sql.SparkSession

object TwitterJoinsMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkPrograms")
      .getOrCreate()

    val sc = spark.sparkContext


    val df = spark.read.option("header", "true").csv("input/Twitter-dataset/data/edges.csv")
    val edgesRDD = df.rdd.map(row => (row.getString(0), row.getInt(1)))


    val groupedRDD = edgesRDD.groupByKey()
    val rddGResult = groupedRDD.mapValues(_.sum).collect()
    println("RDD-G Result:")
    rddGResult.foreach(println)


    val rddRResult = edgesRDD.reduceByKey(_ + _).collect()
    println("\nRDD-R Result:")
    rddRResult.foreach(println)


    val rddFResult = edgesRDD.foldByKey(0)(_ + _).collect()
    println("\nRDD-F Result:")
    rddFResult.foreach(println)


    val zeroValue = 0
    val rddAResult = edgesRDD.aggregateByKey(zeroValue)(_ + _, _ + _).collect()
    println("\nRDD-A Result:")
    rddAResult.foreach(println)


    val dsetResult = edgesRDD.groupByKey().mapValues(_.sum).collect()
    println("\nDSET Result:")
    dsetResult.foreach(println)

    spark.stop()
  }
}