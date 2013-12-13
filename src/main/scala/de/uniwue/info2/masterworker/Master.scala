package de.uniwue.info2.masterworker

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Terminated
import akka.actor.actorRef2Scala
import de.uniwue.info2.masterworker.Master._

class Master extends Actor with ActorLogging {
  import context._

  // an abstract work item and its owner
  type Workload = (ActorRef, Any)

  // -- actor state:
  // workers and their workloads
  var workers = Map.empty[ActorRef, Option[Workload]]

  // queue of pending work items
  var pendingWork = List.empty[Workload]

  override def preStart() = {
    log.debug("Master is starting.")
  }

  /** processes incoming messages and keeps track of pending work */
  def receive = {
    // an actor was created
    case WorkerCreated(worker) => workerCreated(worker)
    // an actor requests work
    case WorkerRequestsWork(worker) => workRequest(worker)
    // a worker is done working
    case WorkIsDone(worker) => workDone(worker)
    // a worker terminated
    case Terminated(worker) => workerTerminated(worker)
    // on any other message create work item
    case m: Any => enqueueWork(sender, m)
  }

  /** enqueue a new work item and notify idling workers*/
  private def enqueueWork(owner: ActorRef, work: Any) = {
    log.info(s"Enqueueing new workitem $work from ${owner.path}")

    pendingWork = (owner, work) :: pendingWork
    notifyWorkers()
  }

  /** after a worker died, its work re-dispatched */
  private def workerTerminated(worker: ActorRef) = {

    workers.get(worker) map {
      case Some((owner, workload)) => {
        log.error("A busy worker died: {}.", worker)
        // reschedule work
        self ! (workload, owner)
      }
      case None => log.error("An idling worker died: {}.", worker)
    }

    workers -= worker
  }

  /** will be called whenever a worker finished its task */
  private def workDone(worker: ActorRef) = {
    if (!workers.contains(worker))
      log.error("I don't know this guy: {}", worker)

    // set worker to idle
    else workers += (worker -> None)
  }

  /** a worker requested work */
  private def workRequest(worker: ActorRef) = {
    log.debug("Worker {} requests work.", worker)

    // tell him to do stuff
    def dispatchWork(a: ActorRef) = {
      pendingWork match {

        // there is work to do
        case (owner, workload) :: xs => {
          // send workload to worker
          log.debug(s"Sending work (${owner.path}, $workload) to worker ${a.path}.")
          a ! WorkToBeDone(owner, workload)
          // continue with reduced workload and save worker state as busy
          pendingWork = pendingWork.tail
          workers += (a -> Some(owner, workload))
        }

        // no work available
        case Nil => a ! NoWorkToBeDone
      }
    }

    // requesting worker should be known to the master and not be busy
    workers.get(worker) map {
      case None => dispatchWork(worker)
      case Some(t) => log.debug("Already busy. Drop request.")
    }
  }

  /** all idling workers will be informed that there is work to be done   */
  private def notifyWorkers() = {
    // list of workers currently idling
    def idleWorkers = for {
      (k, None) <- workers
    } yield k

    if (!pendingWork.isEmpty)
      idleWorkers foreach (_ ! WorkIsReady)
  }

  /** will be called upon worker creation */
  private def workerCreated(worker: ActorRef) = {
    log.info("A worker has been created: {}.", worker)
    // monitor worker
    context watch worker
    // ... and add to map of workers
    workers += (worker -> None)
    // broadcast work, if any
    notifyWorkers()
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