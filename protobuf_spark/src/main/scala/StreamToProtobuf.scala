import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import manish.metrics.NewMetric

object StreamToProtobuf {

  case class MetricScala(metricName: String, value: Double, timestamp: String, host: String, region: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("StreamToProtobufJob")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val kafkaBootstrapServers = "localhost:9092"
    val inputTopic = "metrics-topic"
    val outputTopic = "protobuf-metrics-topic"

    val schema = new StructType()
      .add("metricName", StringType)
      .add("value", DoubleType)
      .add("timestamp", StringType)
      .add("host", StringType)
      .add("region", StringType)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) as json")

    val metrics = df
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")
      .as[MetricScala]

    val protobufMetrics = metrics.map { row =>
      val metric = NewMetric(row.metricName, row.value, row.timestamp, row.host, row.region)
      Base64.encodeBase64String(metric.toByteArray)
    }

    val query = protobufMetrics
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      .option("checkpointLocation", "/Users/manishawasthi/Downloads")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }
}
