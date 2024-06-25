import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object BroadcastExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Broadcast Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Create sample datasets
    val df1 = Seq(
      (1, "Alice", 34),
      (2, "Bob", 45),
      (3, "Cathy", 29)
    ).toDF("id", "name", "age")

    val df2 = Seq(
      (1, "Sales"),
      (2, "Marketing"),
      (3, "IT")
    ).toDF("id", "department")

    // Without Broadcasting
    val startTime1 = System.nanoTime()
    val joinDF1 = df1.join(df2, "id")
    joinDF1.show()

    val aggDF1 = joinDF1.groupBy("department").agg(avg("age").alias("average_age"))
    aggDF1.show()

    val windowSpec = Window.partitionBy("department").orderBy("age")
    val windowDF1 = joinDF1.withColumn("row_number", row_number().over(windowSpec))
    windowDF1.show()
    val endTime1 = System.nanoTime()
    println(s"Execution time without broadcasting: ${(endTime1 - startTime1) / 1e9} seconds")

    // With Broadcasting
    val startTime2 = System.nanoTime()
    val broadcastDF2 = spark.sparkContext.broadcast(df2)
    val joinDF2 = df1.join(broadcastDF2.value, "id")
    joinDF2.show()

    val aggDF2 = joinDF2.groupBy("department").agg(avg("age").alias("average_age"))
    aggDF2.show()

    val windowDF2 = joinDF2.withColumn("row_number", row_number().over(windowSpec))
    windowDF2.show()
    val endTime2 = System.nanoTime()
    println(s"Execution time with broadcasting: ${(endTime2 - startTime2) / 1e9} seconds")

    // Compare DAGs
    println("DAG without broadcasting:")
    joinDF1.explain(true)

    println("DAG with broadcasting:")
    joinDF2.explain(true)

    spark.stop()
  }
}
