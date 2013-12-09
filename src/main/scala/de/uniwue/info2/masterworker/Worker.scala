package de.uniwue.info2.masterworker

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import de.uniwue.info2.masterworker.Master._
import de.uniwue.info2.masterworker.Worker._

abstract class Worker(path: ActorPath) extends Actor with ActorLogging {
  val master = context.actorSelection(path)

  /** will be implemented by concrete worker */
  def doWork(owner: ActorRef, work: Any)

  import context._

  // worker's idle routine
  def idle: Receive = {

    // if work is available request it
    case WorkIsReady => master ! WorkerRequestsWork(self)

    // master replied: do stuff!
    case WorkToBeDone(owner, work) => {
      log.info("Received work-item: {}", work)
      doWork(owner, work)
      become(busy)
    }

    // master replied: no work available
    case NoWorkToBeDone => ()

    // any other message
    case m: Any => log.warning("I received a message I don't understand: {}", m); unhandled(m)
  }

  // worker's busy routine
  def busy: Receive = {

    // if already busy, don't handle any incoming work
    case WorkToBeDone(owner, work) => log.error("I am already working. I should not get any more items! {}", work)

    // i'm done with my work ...
    case Done => {
      log.info("Work complete!")
      // ... notify master 
      master ! WorkIsDone(self)
      // ... request more work
      master ! WorkerRequestsWork(self)
      // become idle
      become(idle)
    }

    // any other message
    case m: Any => log.warning("I received a message I don't understand: {}", m); unhandled(m)
  }

  // start with being idle
  def receive = idle

}

object Worker {

  // send this object to self to become idle again
  case class Done
}