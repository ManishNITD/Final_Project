
import io.circe.generic.auto._
import io.circe.syntax._

import java.time.Instant
import scala.util.Random

case class Metric(metricName: String, value: Double, timestamp: String, host: String, region: String)

object MetricsGenerator {

  private val metricNames = Seq(
    "cpu_usage_percentage",
    "memory_usage_gb",
    "disk_io_rate_mbps",
    "network_throughput_mbps",
    "response_time_ms",
    "error_rate_percentage"
  )

  private val hosts = Seq("server01", "server02", "server03", "server04", "server05")
  private val regions = Seq("us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-south-1")

  def generateMetric(): Metric = {
    val metricName = metricNames(Random.nextInt(metricNames.length))
    val value = metricName match {
      case "cpu_usage_percentage"    => Random.nextInt(101)
      case "memory_usage_gb"         => Random.nextInt(65)
      case "disk_io_rate_mbps"       => Random.nextInt(501)
      case "network_throughput_mbps" => Random.nextInt(1001)
      case "response_time_ms"        => Random.nextInt(2001)
      case "error_rate_percentage"   => Random.nextInt(101)
    }
    val host = hosts(Random.nextInt(hosts.length))
    val region = regions(Random.nextInt(regions.length))
    val timestamp = Instant.now.toString

    Metric(metricName, value, timestamp, host, region)
  }

  def toJson(metric: Metric): String = metric.asJson.noSpaces
}

