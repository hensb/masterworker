package de.uniwue.info2.masterworker

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.actor.actorRef2Scala
import de.uniwue.info2.masterworker.Master.NoWorkToBeDone
import de.uniwue.info2.masterworker.Master.WorkIsDone
import de.uniwue.info2.masterworker.Master.WorkIsReady
import de.uniwue.info2.masterworker.Master.WorkToBeDone
import de.uniwue.info2.masterworker.Master.WorkerCreated
import de.uniwue.info2.masterworker.Master.WorkerRequestsWork
import de.uniwue.info2.masterworker.Worker.DoneWorking
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import akka.actor.Identify
import akka.actor.ActorIdentity
import akka.actor.ReceiveTimeout

abstract class Worker(path: ActorPath) extends Actor with ActorLogging {

  import context._
  import ExecutionContext.Implicits.global

  /** send identification request */
  def identify() = {
    setReceiveTimeout(3 seconds)
    actorSelection(path) ! Identify(path)
  }

  /** will be implemented by concrete worker */
  @throws(classOf[Exception])
  def doWork(owner: ActorRef, work: Any, p: Promise[Unit])

  def booting: Receive = {
    case ActorIdentity(path, Some(master)) => {
      setReceiveTimeout(Duration.Undefined)
      become(idle(master))

      // register this worker with master
      master ! WorkerCreated(self)
    }
    case ActorIdentity(path, None) => log.error("Could not look up master actor.")
    case ReceiveTimeout => identify()
    case _ => log.error("I am receiving messages, but I am not connected to the master yet!")
  }

  // worker's idle routine
  def idle(master: ActorRef): Receive = {

    // if work is available request it
    case WorkIsReady =>
      master ! WorkerRequestsWork(self)

    // master replied: do stuff!
    case WorkToBeDone(owner, work) => {
      log.info(s"Received work-item $work by ${owner.path}.")

      // if work is done at some point in the future, become 'idle' again
      val p = Promise[Unit]()
      val me = self // capture reference to self!
      p.future.onComplete { _ => me ! DoneWorking }

      // perform work
      doWork(owner, work, p)

      // change state to 'busy'
      become(busy(master))
    }

    // master replied: no work available
    case NoWorkToBeDone => ()

    // any other message
    case m: Any => log.warning("While idle I received a message I don't understand: {}", m); unhandled(m)
  }

  // worker's busy routine
  def busy(master: ActorRef): Receive = {

    // if already busy, don't handle any incoming work
    case WorkToBeDone(owner, work) => log.error("I am already working. I should not get any more items! {}", work)

    // i'm done with my work ...
    case DoneWorking => {
      log.info("Work complete!")
      // become idle
      become(idle(master))
      // ... notify master 
      master ! WorkIsDone(self)
      // ... request more work
      master ! WorkerRequestsWork(self)
    }

    // any other message
    case m: Any => log.warning("While busy I received a message I don't understand: {}", m); unhandled(m)
  }

  // start by booting up
  def receive = booting

  // first task: look up (remote) master
  override def preStart = {
    super.preStart

    // send identification request
    identify()
  }
}

object Worker {
  // send this object to self to become idle again
  case class DoneWorking
}