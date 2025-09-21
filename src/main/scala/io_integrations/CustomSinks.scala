package io_integrations

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.io.{FileWriter, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.util.Scanner

object CustomSinks {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stringStream: DataStream[String] = env.fromElements(
    "This is an example of a sink function",
    "This is another string",
    "These sinks are very similar to other things that require sinks"
  )

  // Pushing strings to a file sink

  // instantiated once per thread
  class FileSink(path: String) extends RichSinkFunction[String] {
    /* Use RichFunctions because:
      - they hold state
      - have lifecycle methods. Very useful for file I/O
     */

    var writer: PrintWriter = _

    // called once per event in the datastream
    override def invoke(event: String, context: SinkFunction.Context): Unit = {
      writer.println(event)
      writer.flush()
    }

    override def open(parameters: Configuration): Unit = {
      // initialize resources
      writer = new PrintWriter(new FileWriter(path, true)) // append mode
    }

    override def close(): Unit = {
      // close resources
      writer.close()
    }
  }

  def demoFileSink(): Unit = {
    stringStream.addSink(new FileSink("output/demoFileSink.txt"))
    stringStream.print()
    env.execute()
  }

  /**
   * Exercise: Create a sink function that will push data (as strings) to a socket sink
   */

  class SocketSink(host: String, port: Int) extends RichSinkFunction[String] {
    var socket: Socket = _
    var writer: PrintWriter = _

    override def invoke(value: String, context: SinkFunction.Context): Unit = {
      writer.println(value)
      writer.flush()
    }

    override def open(parameters: Configuration): Unit = {
      socket = new Socket(host, port)
      writer = new PrintWriter(socket.getOutputStream)
    }

    override def close(): Unit = {
      socket.close()
    }
  }

  def demoSocketSink(): Unit = {
    stringStream.addSink(new SocketSink("localhost", 12345)).setParallelism(1)
    stringStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
//    demoFileSink()
    demoSocketSink()
  }

}

object DataReceiver {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(12345)
    println("Waiting for Flink to connect...")
    val socket = server.accept()
    val reader = new Scanner(socket.getInputStream)
    println("Flink connected. Reading...")

    while (reader.hasNextLine) {
      println(s"> ${reader.nextLine()}")
    }

    socket.close()
    println("All data read. Closing app.")
    server.close()
  }
}