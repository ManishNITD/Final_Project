import org.apache.spark.sql.SparkSession

object read_from_keyspace extends App {
  val spark = SparkSession.builder
    .appName("AWS Keyspaces Connection")
    .master("local[*]")
    .config("spark.cassandra.connection.host","cassandra.us-west-2.amazonaws.com")
    .config("spark.cassandra.connection.port", "9142")
    .config("spark.cassandra.connection.ssl.enabled", "true")
    .config("spark.cassandra.auth.username", "xx")
    .config("spark.cassandra.auth.password", "xx")
    .config("spark.cassandra.input.consistency.level", "LOCAL_QUORUM")
    .config("spark.cassandra.connection.ssl.trustStore.path", "/Users/manishawasthi/cassandra_truststore.jks")
    .config("spark.cassandra.connection.ssl.trustStore.password", "xx")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()



  val hadoopConfig = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set("fs.s3a.access.key", "xx")
  hadoopConfig.set("fs.s3a.secret.key", "xx")
  hadoopConfig.set("fs.s3a.endpoint", "s3.amazonaws.com")
  hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

println("reading data from cassandra")

  val df1=spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "air_quality_data", "keyspace" -> "tutorialkeyspace"))
    .load()

  println("data read from cassandra")
  println("writing data in s3 using parquet format")

  df1.write
    .format("parquet")
    .mode("overwrite")
    .save("s3a://spark-write/air_quality")


println("data successfully written to s3, closing spark Session")
  spark.stop()
}
