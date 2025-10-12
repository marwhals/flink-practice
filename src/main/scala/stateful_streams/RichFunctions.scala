package stateful_streams

import generators.shopping.{AddToShoppingCartEvent, SingleShoppingCartEventsGenerator}
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object RichFunctions {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val numberStream: DataStream[Int] = env.fromElements(1, 2, 3, 5, 6, 7, 8)
  // pure FP
  val tenxNumbers: DataStream[Int] = numberStream.map(_ * 10)

  // "explicit" map functions
  val tenxNumbers_v2: DataStream[Int] = numberStream.map(new MapFunction[Int, Int] {
    override def map(value: Int) = value * 10
  })

  // Rich map function
  val tenxNumebrs_v3: DataStream[Int] = numberStream.map(new RichMapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10
  })

  //Rich map functions with overrides for lifecycles
  val tenxNumbersWithLifecycle: DataStream[Int] = numberStream.map(new RichMapFunction[Int, Int] {
    override def map(value: Int) = value * 10 // mandatory override

    // optional overrides: lifecycle methods open/close
    // called before data goes through --- these function will be called even if the job is not executed
    override def open(parameters: Configuration): Unit =
      println("Starting my work!!")

    // invoked after all the data
    override def close(): Unit =
      println("Finishing my work...")
  })

  // ProcessFunction - the most general function abstraction in Flink - will extend other traits
  val tenxNumbersProcess: DataStream[Int] = numberStream.process(new ProcessFunction[Int, Int] {
    override def processElement(value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]) =
      out.collect(value * 10)

    // can also override the lifecycle methods
    override def open(parameters: Configuration): Unit =
      println("Process function starting")

    override def close(): Unit =
      println("Closing process function")
  })

  /**
   * Exercise: "explode" all purchase events to a single item
   * [("boots", 2, (iPhone, 1)] ---> ["boots", "boots", iphone]
   * Using:
   * - Lambdas
   * - Explicit functions
   * - Rich functions
   * - Process functions
   */

  def exercise(): Unit = {
    val exerciseEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartStream: DataStream[AddToShoppingCartEvent] = exerciseEnv.addSource(new SingleShoppingCartEventsGenerator(100))
      .filter(_.isInstanceOf[AddToShoppingCartEvent])
      .map(_.asInstanceOf[AddToShoppingCartEvent])

    // 1 - lambdas
    val itemsPurchasedStream: DataStream[String] = {
      shoppingCartStream.flatMap(event => (1 to event.quantity).map(_ => event.sku))
    }
    // 2 - explicit flatMap function
    val itemsPurchasedStream_v2: DataStream[String] =
      shoppingCartStream.flatMap(new FlatMapFunction[AddToShoppingCartEvent, String] {
        override def flatMap(event: AddToShoppingCartEvent, out: Collector[String]) =
          (1 to event.quantity).map(_ => event.sku).foreach(out.collect)
      })

    // 3 - rich flatMap function
    val itemsPurchasedStream_v3: DataStream[String] =
      shoppingCartStream.flatMap(new RichFlatMapFunction[AddToShoppingCartEvent, String] {
        override def flatMap(event: AddToShoppingCartEvent, out: Collector[String]) =
          (1 to event.quantity).map(_ => event.sku).foreach(out.collect)

        override def open(parameters: Configuration): Unit =
          println("Processing with rich flatMap function")

        override def close(): Unit =
          println("Finishing rich flatMap function")
      })

    // 4 - process function
    val itemsPurchasedStream_v4: DataStream[String] =
      shoppingCartStream.process(new ProcessFunction[AddToShoppingCartEvent, String] {
        override def processElement(event: AddToShoppingCartEvent, ctx: ProcessFunction[AddToShoppingCartEvent, String]#Context, out: Collector[String]) =
          (1 to event.quantity).map(_ => event.sku).foreach(out.collect)
      })

    //    itemsPurchasedStream_v3.print()
    exerciseEnv.execute()
  }


  def main(args: Array[String]): Unit = {
    //    tenxNumbersWithLifecycle.print()
    //    env.execute()
    exercise()
  }
}