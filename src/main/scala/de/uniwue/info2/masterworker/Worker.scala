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

abstract class Worker(path: ActorPath) extends Actor with ActorLogging {
  val master = context.actorSelection(path)

  /** will be implemented by concrete worker */
  @throws(classOf[Exception])
  def doWork(owner: ActorRef, work: Any, p: Promise[Unit])

  import context._
  import ExecutionContext.Implicits.global

  // worker's idle routine
  def idle: Receive = {

    // if work is available request it
    case WorkIsReady =>
      master ! WorkerRequestsWork(self)

    // master replied: do stuff!
    case WorkToBeDone(owner, work) => {
      log.info("Received work-item: {}", work)

      // if work is done at some point in the future, become 'idle' again
      val p = Promise[Unit]()
      val me = self // capture reference to self!
      p.future.onComplete { _ => me ! DoneWorking }

      // perform work
      doWork(owner, work, p)

      // change state to 'busy'
      become(busy)
    }

    // master replied: no work available
    case NoWorkToBeDone => ()

    // any other message
    case m: Any => log.warning("While idle I received a message I don't understand: {}", m); unhandled(m)
  }

  // worker's busy routine
  def busy: Receive = {

    // if already busy, don't handle any incoming work
    case WorkToBeDone(owner, work) => log.error("I am already working. I should not get any more items! {}", work)

    // i'm done with my work ...
    case DoneWorking => {
      log.info("Work complete!")
      // become idle
      become(idle)
      // ... notify master 
      master ! WorkIsDone(self)
      // ... request more work
      master ! WorkerRequestsWork(self)
    }

    // any other message
    case m: Any => log.warning("While busy I received a message I don't understand: {}", m); unhandled(m)
  }

  // start with being idle
  def receive = idle

  // we need to inform the master that we're up and running
  override def preStart = {
    super.preStart
    master ! WorkerCreated(self)
  }

}

object Worker {
  // send this object to self to become idle again
  case class DoneWorking
}