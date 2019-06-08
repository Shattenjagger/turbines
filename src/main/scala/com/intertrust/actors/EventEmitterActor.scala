package com.intertrust.actors

import java.io.InputStream
import java.time.Instant
import java.time.temporal.ChronoUnit
import akka.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  Cancellable,
  PoisonPill,
  Props
}
import akka.pattern.ask
import akka.persistence.{PersistentActor, SnapshotOffer}
import akka.util.Timeout
import com.intertrust.parsers.{MovementEventParser, TurbineEventParser}
import com.intertrust.protocol.{MovementEvent, TurbineEvent}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source

class EventEmitterActor(val movementsStream: InputStream,
                        val turbinesStream: InputStream)
    extends Actor
    with ActorLogging {
  import context.dispatcher
  import EventEmitterActor._

//  override def persistenceId = "event-emitter-actor"

  implicit val timeout: Timeout = 5.seconds

  val speedCoefficient: Int = ConfigFactory.load().getInt("speedCoefficient")

  val stateActor: ActorRef =
    context.system.actorOf(Props(classOf[StateActor]), "stateActor")

  val movementEvents: Iterator[MovementEvent] = new MovementEventParser()
    .parseEvents(Source.fromInputStream(movementsStream))

  val turbineEvents: Iterator[TurbineEvent] =
    new TurbineEventParser()
      .parseEvents(Source.fromInputStream(turbinesStream))

  var currentMovementEvent: MovementEvent = _
  var currentTurbineEvent: TurbineEvent = _
  var simulatedTime: Instant = _

//  var state = ActorState()
  var ticks: Int = 0

  def nextTick: Cancellable =
    context.system.scheduler
      .scheduleOnce(1.minute / speedCoefficient, self, Tick)
//
//  val receiveRecover: Receive = {
////    case evt: Evt                                 => updateState(evt)
//    case SnapshotOffer(_, snapshot: ActorState) =>
//      log.info(s"Got recover $snapshot")
//      state = snapshot
//  }
//
//  override def preStart(): Unit = {
//    log.info("Actor prestart")
//  }

  override def postStop(): Unit = {
    context.system.terminate()
  }

  override def receive: Receive = {
    case Start =>
      log.info(
        s"Event emitter started with speed coefficient $speedCoefficient"
      )

      currentMovementEvent = nextMovementEvent
      currentTurbineEvent = nextTurbineEvent

      if (currentMovementEvent == null && currentTurbineEvent == null) {
        // Edge case when both of sources are empty
        log.info("No events to process")
        self ! PoisonPill
      } else {
        // Trying to realize simulation start date
        (currentMovementEvent, currentTurbineEvent) match {
          case (m: MovementEvent, null) =>
            // Edge case when turbine events source is empty
            simulatedTime = m.timestamp
            nextTick
          case (null, t: TurbineEvent) =>
            // Edge case when movements events source is empty
            simulatedTime = t.timestamp
            nextTick
          case (m, t) =>
            // Earliest date goes to be starting point of our simulation
            simulatedTime =
              if (m.timestamp.compareTo(t.timestamp) > 0) t.timestamp
              else m.timestamp
        }

        nextTick
      }

    case Tick =>
      // Asking state actor to update turbines statuses and send proper alerts
      // We don't want to clash it with new events, so waiting until it complete the update
      Await.result(
        stateActor ? StateActor.UpdateStatus(simulatedTime),
        timeout.duration
      )

      // Increasing simulation time
      simulatedTime = simulatedTime.plus(1, ChronoUnit.MINUTES)
      log.info(s"Simulated time: $simulatedTime")

      // Starting to process new events
      self ! NextIteration

    case NextIteration =>
      // One iteration is emitting one or none events. If no events was emitted then we're ready to go to next tick

      (currentMovementEvent, currentTurbineEvent) match {
        case (null, null) =>
          log.info("No more unprocessed events left, going to kill self")
          self ! PoisonPill
        case (m: MovementEvent, null) =>
          // We have no more turbine events, continue emit movements

          if (currentMovementEvent.timestamp.compareTo(simulatedTime) < 0) {
            stateActor ! m
            currentMovementEvent = nextMovementEvent
            self ! NextIteration
          } else
            nextTick
        case (null, t: TurbineEvent) =>
          // We have no more movement events, continue emit turbine events

          if (currentTurbineEvent.timestamp.compareTo(simulatedTime) < 0) {
            stateActor ! t
            currentTurbineEvent = nextTurbineEvent
            self ! NextIteration
          } else
            nextTick
        case (m, t) =>
          // Earliest event goes to be emitted if it's before current simulated date
          // If both of dates are later than simulated date then we're going to next tick

          if (m.timestamp.compareTo(t.timestamp) < 0) {
            if (m.timestamp.compareTo(simulatedTime) < 0) {
              stateActor ! m
              currentMovementEvent = nextMovementEvent
              self ! NextIteration
            } else nextTick
          } else {
            if (t.timestamp.compareTo(simulatedTime) < 0) {
              stateActor ! t
              currentTurbineEvent = nextTurbineEvent
              self ! NextIteration
            } else nextTick
          }
      }
  }

  def nextMovementEvent: MovementEvent =
    if (movementEvents.hasNext) movementEvents.next() else null
  def nextTurbineEvent: TurbineEvent =
    if (turbineEvents.hasNext) turbineEvents.next() else null
}

object EventEmitterActor {
  case object Start
  case object Tick
  case object NextIteration

//  case class ActorState(ticks: Int = 0)
}
