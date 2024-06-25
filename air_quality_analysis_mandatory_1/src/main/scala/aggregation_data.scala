import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, mean, sum}

object agg extends App {
  val spark = SparkSession.builder
    .appName("AWS Keyspaces Connection")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()



  val hadoopConfig = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set("fs.s3a.access.key", "xx")
  hadoopConfig.set("fs.s3a.secret.key", "xx")
  hadoopConfig.set("fs.s3a.endpoint", "s3.amazonaws.com")
  hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  println("reading from s3")

  val df1= spark
    .read
    .format("parquet")
    .load("s3a://spark-write/air_quality")

  print(df1.count())
  df1.describe().show()
  df1.groupBy("Date").agg(mean("Temp"),avg("soil Temp")).show()
  val windowspec=Window.partitionBy("station_name").orderBy("date")
  val df2=df1.withColumn("window_sum",sum("O3").over(windowspec))
  df2.show()

  spark.stop()

}