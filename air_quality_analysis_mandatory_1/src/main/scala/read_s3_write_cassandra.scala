import org.apache.spark.sql.SparkSession

object WriteToKeyspaces extends App {
  val spark = SparkSession.builder
    .appName("AWS Keyspaces Connection")
    .master("local[*]")
    .config("spark.cassandra.connection.host","cassandra.us-west-2.amazonaws.com")
    .config("spark.cassandra.connection.port", "9142")
    .config("spark.cassandra.connection.ssl.enabled", "true")
    .config("spark.cassandra.auth.username", "xx")
    .config("spark.cassandra.auth.password", "xx")
    .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
    .config("spark.cassandra.connection.ssl.trustStore.path", "/Users/manishawasthi/cassandra_truststore.jks")
    .config("spark.cassandra.connection.ssl.trustStore.password", "xx")
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
    .option("inferschema","true")
    .option("header","true")
    .csv("s3a://s3-manishnitd/zaragoza_data.csv")

  println("successfully read from s3, writing to cassandra")
  println("printing the schema of dataframe")

  df1.printSchema()


  df1.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "air_quality_data", "keyspace" -> "tutorialkeyspace"))
    .mode("append")
    .save()

  println("successfully written to cassandra, stopping spark session")

  spark.stop()
}
