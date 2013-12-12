package de.uniwue.info2.masterworker.messagebounce

import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.util.Random
import akka.actor.ActorLogging
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import de.uniwue.info2.masterworker.Master
import de.uniwue.info2.masterworker.Worker
import akka.actor.PoisonPill
import akka.routing.RoundRobinRouter

object BounceMessages extends App {

  override def main(args: Array[String]) {

    val system = ActorSystem("system")
    val master = system.actorOf(Master.mkProps(), "master")
    val worker = system.actorOf(MyWorker.mkProps(master.path).
      withRouter(RoundRobinRouter(5)), "workerRouter")

    master ! "peng"
    master ! "zack1"
    master ! "zack2"
    master ! "zack3"
    master ! "bumm"
    master ! "derp"
  }
}

class MyWorker(path: ActorPath) extends Worker(path: ActorPath) with ActorLogging {

  def doWork(owner: ActorRef, work: Any, p: Promise[Unit]) = {

    Thread.sleep(Random.nextInt(2000))
    p.success()
  }
}
object MyWorker {
  def mkProps(path: ActorPath) = Props(new MyWorker(path))
}