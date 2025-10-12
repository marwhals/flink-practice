package stateful_streams

import generators.shopping.{AddToShoppingCartEvent, ShoppingCartEvent, SingleShoppingCartEventsGenerator}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{CheckpointListener, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.nio.file.Paths

/**
 * Checkpoints = the entire state at an exact point in time
 * --- Need to consider that a distributed system is inherently unreliable
 * --- Need to consider failures
 * ----- Killed processes, failed/ unreachable machines
 *
 * Checkpointing - Naive approach
 * Taking a checkpoint
 * - Pause the application and data ingestion
 * - Wait for in-flight data to be processed
 * - Copy the state to the checkpoint backend
 * - Resume data ingestion
 * Restoring a checkpoint after failure
 * - Restart the entire application
 * - Copy the checkpoint data to all the stateful tasks
 * - Resume data ingestion
 * In modern data infrastructures, this approach is unacceptable
 * - Prolonged "hiccups" in data updates i.e data increased latency
 * - The incoming data may be too much to handle after resuming
 *
 * Checkpointing in Flink
 * A rolling checkpoint algorithm
 * - Checkpoint barrier emitted in line with the data
 * - As the barrier arrives at tasks, they store their state
 * - Incoming elements are buffered until state is stored
 * Steps (TODO include a diagram)
 * - The stream is running
 * - The JobManager adds a checkpoint barrier (checkpoint element)
 * - Tasks saves its state and forwards the barrier
 * - Barrier arrives at tasks, they save their state
 * - If new elements arrive while saving, buffer them
 * - Barrier moves forward, elements continue to flow
 * - The sinks acknowledges (acks) the checkpoint to the JobManager. I.e checkpoint is done when the checkpoint barrier reaches the sink.
 * - Checkpoint complete, move on
 * ------ TODO see paper on the flink checkpointing algorithm
 *
 */

object Checkpoints {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // set checkpoint intervals
  env.getCheckpointConfig.setCheckpointInterval(5000) // a checkpoint triggered every 5s

  // Resolve a path relative to your repo root
  val checkpointPath = Paths.get("output/checkpoints").toAbsolutePath.toUri.toString

  // set checkpoint storage
  env.getCheckpointConfig.setCheckpointStorage(checkpointPath)


  /**
   * Scenario: Keep track of the number of AddToCart events per user, when quantity > a threshold (e.g managing stock)
   * Persist the data (state) via checkpoints
   */

  val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(sleepMillisBetweenEvents = 100, generateRemoved = true))

  val eventsByUser = shoppingCartEvents
    .keyBy(_.userId)
    .flatMap(new HighQuantityCheckpointedFunction(5))

  def main(args: Array[String]): Unit = {
    eventsByUser.print()
    env.execute()
  }
}

class HighQuantityCheckpointedFunction(val threshold: Long)
  extends FlatMapFunction[ShoppingCartEvent, (String, Long)]
    with CheckpointedFunction
    with CheckpointListener {

  var stateCount: ValueState[Long] = _ // instantiated per key

  override def flatMap(event: ShoppingCartEvent, out: Collector[(String, Long)]): Unit = {
    event match {
      case AddToShoppingCartEvent(userId, sku, quantity, time) =>
        if (quantity > threshold) {
          // updated state
          val newUserEventCount = stateCount.value() + 1
          stateCount.update(newUserEventCount)

          // push output
          out.collect((userId, newUserEventCount))

        }
      case _ => // do nothing
    }
  }

  // invoked when the checkpoint is triggered
  override def snapshotState(context: FunctionSnapshotContext): Unit =
    println(s"""checkpoint at ${context.getCheckpointTimestamp}""")

  // lifecycle method to initialised state (roughly equal in RichFunctions)
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stateCountDescriptor = new ValueStateDescriptor[Long]("impossibleOrderCount", classOf[Long])
    stateCount = context.getKeyedStateStore.getState(stateCountDescriptor)
  }

  override def notifyCheckpointComplete(checkpointId: Long): Unit = ()
  override def notifyCheckpointAborted(checkpointId: Long): Unit = ()
}