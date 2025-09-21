package io_integrations

import generators.shopping.{ShoppingCartEvent, SingleShoppingCartEventsGenerator}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object SideOutputs {

  /**
   * Scenario: Shopping cart events
   * - We want to process this in two different ways
   * - E.g events for "Alice" and all the events of everyone else
   */

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(100))

  // This can be done using output tags - these are only available for output functions
  val aliceTag = new OutputTag[ShoppingCartEvent]("alice-events") // name needs to be unique

  class AliceEventsFunction extends ProcessFunction[ShoppingCartEvent, ShoppingCartEvent] {
    override def processElement(
                                 event: ShoppingCartEvent,
                                 ctx: ProcessFunction[ShoppingCartEvent, ShoppingCartEvent]#Context,
                                 out: Collector[ShoppingCartEvent] // "primary" destination / datastream
                               ): Unit = {
      if (event.userId == "Alice") {
        ctx.output(aliceTag, event) // collecting an event through a secondary destination
      } else {
        out.collect(event)
      }
    }
  }

  def demoSideOutput(): Unit = {
    val allEventsButAlices: DataStream[ShoppingCartEvent] = shoppingCartEvents.process(new AliceEventsFunction)
    val alicesEvents: DataStream[ShoppingCartEvent] = allEventsButAlices.getSideOutput(aliceTag)

    // process the datastreams separately
    alicesEvents.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoSideOutput()
  }

}
