package homework

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, Zip}


object Graph {
  implicit val system = ActorSystem("fusion")
  implicit val materializer = ActorMaterializer()
  val graph = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>

    import GraphDSL.Implicits._

    val input = builder.add(Source(1 to 1000))
    val multiTen = builder.add(Flow[Int].map(x => x * 10))
    val multiTwo = builder.add(Flow[Int].map(x => x * 2))
    val multiThree = builder.add(Flow[Int].map(x => x * 3))
    val merge = builder.add(Flow[((Int, Int), Int)].map(x => (x._1._1, x._1._2, x._2)))
    val reduce = builder.add(Flow[(Int, Int, Int)].map(x => x._1 + x._2 + x._3))
    val output = builder.add(Sink.foreach(println))

    val broadcast = builder.add(Broadcast[Int](3))
    val zip = builder.add(Zip[Int, Int])
    val zip2 = builder.add(Zip[(Int, Int), Int])

    input ~> broadcast

    broadcast.out(0) ~> multiTen ~> zip.in0
    broadcast.out(1) ~> multiTwo ~> zip.in1
    zip.out ~> zip2.in0
    broadcast.out(2) ~> multiThree ~> zip2.in1
    zip2.out ~> merge ~> reduce ~> output

    ClosedShape
  }

  def main(args: Array[String]) : Unit ={
    RunnableGraph.fromGraph(graph).run()

  }
}