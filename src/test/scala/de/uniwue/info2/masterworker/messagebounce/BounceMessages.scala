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

    System.err.println("LAUNCHING");

    val system = ActorSystem("system")
    val master = system.actorOf(Master.mkProps(), "master")
    val worker = system.actorOf(MyWorker.mkProps(master.path).
      withRouter(RoundRobinRouter(5)), "workerRouter")

    master ! "peng1"
    master ! "peng2"
    master ! "peng3"
    master ! "peng4"
    master ! "peng5"
    master ! "peng6"
    master ! "peng7"
  }
}

class MyWorker(path: ActorPath) extends Worker(path: ActorPath) with ActorLogging {

  override def timeout = 500 milliseconds

  override def doWork(owner: ActorRef, work: Any, p: Promise[Unit]) = work match {

    case x: String => {
      if (0.5 >= Random.nextDouble)
        throw new IllegalStateException("Trollololol. U MAD, BRO?")

      Thread.sleep(Random.nextInt(2000))
      p.success()
    }

    case _ => System.err.println(s"WTF? $work");
  }
}
object MyWorker {
  def mkProps(path: ActorPath) = Props(new MyWorker(path))
}