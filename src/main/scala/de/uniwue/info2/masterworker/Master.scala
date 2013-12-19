package de.uniwue.info2.masterworker

import scala.concurrent.duration.FiniteDuration

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Restart
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

  // workers and their scheduled timeouts
  var pending = Map.empty[ActorRef, Cancellable]

  // queue of pending work items
  var pendingWork = List.empty[Workload]

  var count = 0

  override def preStart() = {
    log.debug("Master is starting.")
  }

  // in case of any error, restart the worker
  override val supervisorStrategy = OneForOneStrategy() {
    case _ => Restart
  }

  /** processes incoming messages and keeps track of pending work */
  def receive = {
    // an actor was created
    case WorkerCreated(worker) => workerCreated(worker)
    // an actor requests work
    case WorkerRequestsWork(worker, timeout) => workRequest(worker, timeout)
    // a worker is done working
    case WorkIsDone(worker) => workDone(worker)
    // a worker terminated
    case Terminated(worker) => workerTerminated(worker)
    // a timeout needs to be checked
    case CheckTimeout(worker, owner, work) => checkTimeout(worker, owner, work)
    // on any other message create work item
    case m: Any => enqueueWork(sender, m)
  }

  private def checkTimeout(worker: ActorRef, owner: ActorRef, work: Any) = {
    log.info(s"Checking timeout for ${worker.path}")

    workers.get(worker) map {
      case Some((curOwn, curWork)) => {
        // if worker is still working on the same task for the same owner, 
        // set worker to idle and re-schedule the work
        log.info(s"worker took too long. rescheduling his work.")
        if (curWork == work && owner == curOwn) {
          setIdling(worker)
          self tell (work, owner)
        }
      }
      case None =>
    }
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
        log.error("A busy worker died: {}. Rescheduling his work.", worker)
        // reschedule work
        self tell (workload, owner)
        // remove worker
        workers -= worker
        // cancel timeout checks
        pending.get(worker) map (_.cancel)
        pending -= worker
      }
      case None => log.error("An idling worker died: {}.", worker)
    }

    workers -= worker
  }

  /** will be called whenever a worker finished its task */
  private def workDone(worker: ActorRef) = {
    if (!workers.contains(worker))
      log.debug(s"I don't know this guy: ${worker.path}, requesting handshake.")

    else setIdling(worker)
  }

  private def setIdling(worker: ActorRef) {
    // stop timeout check for task
    pending.get(worker) foreach (_.cancel)
    // remove worker from pending list
    pending -= worker
    // set worker to idle
    workers += (worker -> None)
  }

  /** a worker requested work */
  private def workRequest(worker: ActorRef, timeout: FiniteDuration) = {
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

          // schedule a timeout message for this operation
          val me = self
          val c = system.scheduler.scheduleOnce(timeout) {
            me ! CheckTimeout(a, owner, workload)
          }
          pending += (a -> c)
        }

        // no work available
        case Nil => a ! NoWorkToBeDone
      }
    }

    // requesting worker should be known to the master and not be busy
    workers.get(worker) map {
      case None => dispatchWork(worker)
      case Some(t) => log.debug("This worker is already busy. Drop request.")
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

    // if we already know this worker, it might have restarted
    workers.get(worker) map {

      // reschedule the work
      case Some((owner, workload)) => {
        self ! (workload, owner)
        pending.get(worker) map (_.cancel)
        pending -= worker
      }

      // add worker as idle
      case None => {
        log.info("A worker has been created: {}.", worker)
        // monitor worker
        context watch worker
      }
    }

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

  case class WorkerRequestsWork(w: ActorRef, t: FiniteDuration)

  case class WorkIsReady

  case class NoWorkToBeDone

  case class WorkToBeDone(owner: ActorRef, work: Any)

  case class WorkIsDone(w: ActorRef)

  case class Result(result: Any)

  case class CheckTimeout(w: ActorRef, o: ActorRef, work: Any)
}