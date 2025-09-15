package datastreams

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector // import TypeInformation / implicits for the data of your DataStreams

/**
 * DataStreams
 * - The fundemental abstraction of a stream in Flink
 * - Can be transformed with FP
 * --- map
 * --- flatMap
 * --- filter
 * --- reduce
 * --- process
 *
 * Flink Application
 * - Needs an environment to run
 * ---- Rich data structure with access to all Flink APIs
 * ---- Description of all streams and transformations
 * ---- Lazy evaluation
 *
 *
 */

object EssentialStreams {

  def applicationTemplate(): Unit = {

    // To run flink need 1) an execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // in between, add any sort of computations
    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    simpleNumberStream.print()

    //at the end
    env.execute()
  }

  def demoTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6, 7)

    //checking parallelism
    println(s"Current parallelism --- ${env.getParallelism} ")
    // set different parallelism if we want a different number of files -- this can be set at any step in the data transformation
    env.setParallelism(2)
    println(s"New parallelism --- ${env.getParallelism} ")
    // map
    val doubledNumbers: DataStream[Int] = numbers.map(_ * 2)

    // flatMap
    val expandedNumbers: DataStream[Int] = numbers.flatMap(n => List(n, n + 1))

    //filter
    val filteredNumbers: DataStream[Int] = numbers.filter(_ % 2 == 0)
      /** can set parallelism here */
      .setParallelism(3)

    expandedNumbers.writeAsText("output/expandedStream") // directory with multiple files
      //set parallelism in the sink
      .setParallelism(1)

    env.execute()

  }

  // explicit transformations
  def demoExplicitTransformation(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers = env.fromSequence(1, 100)

    // map
    val doubledNumbers = numbers.map(_ * 2)

    // explicit version
    val doubledNumbers_v2 = numbers.map(new MapFunction[Long, Long] {
      // declare fields, methods, ...
      override def map(value: Long) = value * 2
    })

    // flatMap
    val expandedNumbers = numbers.flatMap(n => Range.Long(1, n, 1).toList)

    //explicit version
    val expandedNumbers_v2 = numbers.flatMap(new FlatMapFunction[Long, Long] {
      // declare fields, methods etc
      /** Collector is a stateful data collector */
      override def flatMap(n: Long, out: Collector[Long]) =
        Range.Long(1, n, 1).foreach { i =>
          out.collect(i) // imperative style
        }
    })

    // process method
    // ProcessFunction is the *most general* function to process elements in flink
    val expandedNumbers_v3 = numbers.process(new ProcessFunction[Long, Long] {
      override def processElement(value: Long, ctx: ProcessFunction[Long, Long]#Context, out: Collector[Long]) =
        Range.Long(1, value, 1).foreach { i =>
          out.collect(i)
        }
    })

    // reduce
    // happens on keyed streams -- essentially hashmaps that are streams
    val keyedNumbers: KeyedStream[Long, Boolean] = numbers.keyBy(n => n % 2 == 0)
    // reduce by FP approach
    val sumByKey = keyedNumbers.reduce(_ + _) // sum up all the elements *buy key*
    val sumByKey_v2 = keyedNumbers.reduce(new ReduceFunction[Long] {
      // can have additional fields and methods
      override def reduce(x: Long, y: Long): Long = x + y
    })

    sumByKey_v2.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoTransformations()
  }

}
