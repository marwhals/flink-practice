package datastreams

import generators.shopping.{ShoppingCartEvent, SingleShoppingCartEventsGenerator}
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Partitions {

  // splitting = partitioning
  def demoPartitioner(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(100)) // ~10 events/s

    // partitioner = logic to split the data
    val partitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = { // invoked on every event
        // hash code % number of partitions ~ even distribution
        println(s"Number of max partitions: $numPartitions")
        key.hashCode % numPartitions
      }
    }

    /**
     * Bad because
     * - you lose parallelism -- use env.setParalellism(1)
     * - you risk overloading the task with the disproportionate data
     *
     * Can be useful if you want to send HTTP requests
     */

    val badPartitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = {
        numPartitions - 1 // last partition index
      }
    }

    val partitionedStream = shoppingCartEvents.partitionCustom(
      partitioner,
      event => event.userId
    )

    val badPartitionedStream = shoppingCartEvents.partitionCustom(
      badPartitioner,
      event => event.userId
    )
    // redistribution of data evenly -- shuffling involves data transfer through the network.
    .shuffle

    partitionedStream.print()
    env.execute()


  }

  def main(args: Array[String]): Unit = {
    demoPartitioner()
  }

}
