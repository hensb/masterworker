package de.uniwue.info2.masterworker.messagebounce

import scala.concurrent.duration.DurationInt
import akka.actor.ActorLogging
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import de.uniwue.info2.masterworker.Master
import de.uniwue.info2.masterworker.Worker
import de.uniwue.info2.masterworker.Worker.Done
import scala.util.Random

object BounceMessages extends App {

  override def main(args: Array[String]) {

    val system = ActorSystem("system")
    val master = system.actorOf(Master.mkProps(), "master")
    val worker = system.actorOf(MyWorker.mkProps(master.path), "worker")

    master ! "peng"
    master ! "zack"
    master ! "bumm"
    master ! "derp"
  }
}

class MyWorker(path: ActorPath) extends Worker(path: ActorPath) with ActorLogging {

  def doWork(owner: ActorRef, work: Any) = {
    log.info("lol - doing work!")

    Thread.sleep(Random.nextInt(2000))

    self ! Done
  }
}
object MyWorker {
  def mkProps(path: ActorPath) = Props(new MyWorker(path))
}