package de.uniwue.info2.masterworker

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import de.uniwue.info2.masterworker.Master._
import akka.actor.Terminated

class Master extends Actor with ActorLogging {
  import context._

  // an abstract work item and its owner
  type Workload = (ActorRef, Any)

  override def preStart() = {
    log.debug("Master is starting.")
  }

  /** processes incoming messages and keeps track of pending work */
  def workWith(pendingWork: List[(ActorRef, Any)], workers: Map[ActorRef, Option[Workload]]): Receive = {
    // an actor was created
    case WorkerCreated(worker) =>
      become(workWith(pendingWork, workerCreated(worker, pendingWork, workers)))
    // an actor requests work
    case WorkerRequestsWork(worker) => workRequest(worker, pendingWork, workers)
    // a worker is done working
    case WorkIsDone(worker) => workDone(worker, workers)
    // a worker terminated
    case Terminated(worker) => become(workWith(pendingWork, workerTerminated(worker, workers)))
  }

  // start with empty work queue
  def receive = workWith(List(), Map())

  /** after a worker died, its work re-dispatched */
  private def workerTerminated(worker: ActorRef, workers: Map[ActorRef, Option[Workload]]) = {

    workers.get(worker) map {
      case Some((owner, workload)) => {
        log.error("A worker died: {}.", worker)
        // reschedule work
        self ! (workload, owner)
      }
      case None =>
    }

    workers - worker
  }

  /** will be called whenever a worker finished its task */
  private def workDone(worker: ActorRef, workers: Map[ActorRef, Option[Workload]]) = {
    if (!workers.contains(worker))
      log.error("I don't know this guy: {}", worker)

    // set worker to idle
    else workers + (worker -> None)
  }

  /** a worker requested work */
  private def workRequest(worker: ActorRef, work: List[Workload], workers: Map[ActorRef, Option[Workload]]) = {
    log.debug("Worker {} requests work.", worker)

    // requesting worker should be known to the master
    workers(worker) map { _ =>
      work match {
        case (owner, workload) :: xs => {
          // send workload to worker
          worker ! WorkToBeDone(owner, workload)
          // continue with reduced workload and save worker state as busy 
          become(workWith(xs, workers + (worker -> Some(owner, workload))))
        }
        case Nil => worker ! NoWorkToBeDone
      }
    }
  }

  /** will be called upon worker creation */
  private def workerCreated(worker: ActorRef, work: List[Workload], workers: Map[ActorRef, Option[Workload]]) = {
    // list of workers currently idling
    def idleWorkers = workers filter { case (worker, load) => load == None } keys

    log.debug("A worker has been created: {}.", worker)
    // monitor worker
    context watch worker
    // ... and add to map of workers
    val newWorkers = workers + (worker -> None)
    // broadcast work, if any
    if (!work.isEmpty)
      idleWorkers foreach (_ ! WorkIsReady)
    // return new workers
    newWorkers
  }
}

object Master {

  case class WorkerCreated(w: ActorRef)

  case class WorkerRequestsWork(w: ActorRef)

  case class WorkIsReady

  case class NoWorkToBeDone

  case class WorkToBeDone(owner: ActorRef, work: Any)

  case class WorkIsDone(w: ActorRef)
}