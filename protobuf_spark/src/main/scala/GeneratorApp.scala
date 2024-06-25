
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.{Sink, Source}
import akka.NotUsed
import akka.stream.Materializer

import scala.concurrent.duration._

object GeneratorApp {

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "MetricsGeneratorSystem")
    implicit val ec = system.executionContext
    implicit val materializer: Materializer = Materializer(system)

    val kafkaService = new KafkaService("localhost:9092", "metrics-topic")

    Source.tick(0.seconds, 5.seconds, NotUsed)
      .map(_ => MetricsGenerator.generateMetric())
      .runWith(Sink.foreach(metric => kafkaService.send(metric)))

    system.log.info("Server Metrics Microservice started")
    println("hello")
  }
}
