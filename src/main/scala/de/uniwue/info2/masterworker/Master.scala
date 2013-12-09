package de.uniwue.info2.masterworker

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import de.uniwue.info2.masterworker.Master._
import akka.actor.Terminated
import akka.actor.Props

class Master extends Actor with ActorLogging {
  import context._

  // an abstract work item and its owner
  type Workload = (ActorRef, Any)

  override def preStart() = {
    log.debug("Master is starting.")
  }

  /** processes incoming messages and keeps track of pending work */
  private def workWith(pendingWork: List[(ActorRef, Any)], workers: Map[ActorRef, Option[Workload]]): Receive = {
    // an actor was created
    case WorkerCreated(worker) =>
      become(workWith(pendingWork, workerCreated(worker, pendingWork, workers)))
    // an actor requests work
    case WorkerRequestsWork(worker) => workRequest(worker, pendingWork, workers)
    // a worker is done working
    case WorkIsDone(worker) => workDone(worker, workers)
    // a worker terminated
    case Terminated(worker) => become(workWith(pendingWork, workerTerminated(worker, workers)))
    // on any other message create work item
    case m: Any => become(workWith(enqueueWork(sender, m, pendingWork, workers), workers))
  }

  // start with empty work queue
  def receive = workWith(List(), Map())

  /** enqueue a new work item and notify idling workers*/
  private def enqueueWork(owner: ActorRef, work: Any, pendingWork: List[Workload],
    workers: Map[ActorRef, Option[Workload]]) = {
    log.info("Enqueueing new workitem {}", work)

    notifyWorkers(pendingWork, workers)
    (sender, work) :: pendingWork
  }

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
    if (workers.contains(worker)) {
      work match {
        case (owner, workload) :: xs => {
          System.err.println("sending " + worker)
          // send workload to worker
          worker ! WorkToBeDone(owner, workload)
          // continue with reduced workload and save worker state as busy 
          become(workWith(xs, workers + (worker -> Some(owner, workload))))
        }
        case Nil => worker ! NoWorkToBeDone
      }
    }
  }

  /** all idling workers will be informed that there is work to be done   */
  private def notifyWorkers(work: List[Workload], workers: Map[ActorRef, Option[Workload]]) = {
    // list of workers currently idling
    def idleWorkers = workers filter { case (worker, load) => load == None } keys

    if (!work.isEmpty)
      idleWorkers foreach (_ ! WorkIsReady)
  }

  /** will be called upon worker creation */
  private def workerCreated(worker: ActorRef, work: List[Workload], workers: Map[ActorRef, Option[Workload]]) = {
    log.debug("A worker has been created: {}.", worker)
    // monitor worker
    context watch worker
    // ... and add to map of workers
    val newWorkers = workers + (worker -> None)
    // broadcast work, if any
    notifyWorkers(work, newWorkers)
    // return new workers
    newWorkers
  }
}

object Master {

  /** factory method */
  def mkProps() = Props[Master]

  /* case classes */
  case class WorkerCreated(w: ActorRef)

  case class WorkerRequestsWork(w: ActorRef)

  case class WorkIsReady

  case class NoWorkToBeDone

  case class WorkToBeDone(owner: ActorRef, work: Any)

  case class WorkIsDone(w: ActorRef)
}