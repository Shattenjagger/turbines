package com.intertrust.actors

import java.io.InputStream
import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.actor.{ActorLogging, ActorRef, Cancellable, PoisonPill, Props}
import akka.pattern.ask
import akka.persistence.{PersistentActor, SaveSnapshotSuccess, SnapshotOffer}
import akka.util.Timeout
import com.intertrust.parsers.{MovementEventParser, TurbineEventParser}
import com.intertrust.protocol.{MovementEvent, TurbineEvent}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.io.Source

class EventEmitterActor(val movementsStream: InputStream,
                        val turbinesStream: InputStream)
    extends PersistentActor
    with ActorLogging {
  import EventEmitterActor._
  import context.dispatcher

  override def persistenceId = "event-emitter-actor"

  implicit val timeout: Timeout = 5.seconds

  val speedCoefficient: Int = ConfigFactory.load().getInt("speedCoefficient")

  val stateActor: ActorRef =
    context.system.actorOf(Props(classOf[StateActor]), "stateActor")

  var movementEvents: Iterator[MovementEvent] = _
  var turbineEvents: Iterator[TurbineEvent] = _

  var currentMovementEvent: MovementEvent = _
  var currentTurbineEvent: TurbineEvent = _
  var simulatedTime: Instant = _

  var state = ActorState()
  var nextStage: ActorStage = _

  def nextTick: Cancellable =
    context.system.scheduler
      .scheduleOnce(1.minute / speedCoefficient, self, Tick)

  def receiveRecover: Receive = {
    case SnapshotOffer(_, snapshot: ActorState) =>
      log.info(s"Got recover $snapshot")
      state = snapshot
    case s => log.info(s"Received state event $s")
  }

  override def postStop(): Unit = {
    log.info(s"Event emitter stopped")
    context.system.terminate()
  }

  override def receiveCommand: Receive = {
    case Start =>
      log.info(
        s"Event emitter started with speed coefficient $speedCoefficient"
      )
      turbineEvents = new TurbineEventParser()
        .parseEvents(Source.fromInputStream(turbinesStream))
        .drop(state.consumedTurbineEvents)
      movementEvents = new MovementEventParser()
        .parseEvents(Source.fromInputStream(movementsStream))
        .drop(state.consumedMovementEvents)

      currentMovementEvent = nextMovementEvent
      currentTurbineEvent = nextTurbineEvent

      if (currentMovementEvent == null && currentTurbineEvent == null) {
        // Edge case when both of sources are empty
        killSelf()
      } else {
        if (state.simulatedTime == null) {
          // Trying to realize simulation start date
          (currentMovementEvent, currentTurbineEvent) match {
            case (m: MovementEvent, null) =>
              // Edge case when turbine events source is empty
              simulatedTime = m.timestamp
            case (null, t: TurbineEvent) =>
              // Edge case when movements events source is empty
              simulatedTime = t.timestamp
            case (m, t) =>
              // Earliest date goes to be starting point of our simulation
              simulatedTime =
                if (m.timestamp.compareTo(t.timestamp) > 0) t.timestamp
                else m.timestamp
          }
        } else {
          // We have dirty state recovered, so we may start from it
          simulatedTime = state.simulatedTime
        }
        nextTick
      }

    case Tick =>
      // Saving current simulation time
      nextStage = UpdateChildStatus
      stateUpdateSimulatedTime()

    case UpdateChildStatus =>
      // Asking state actor to update turbines statuses and send proper alerts
      // We don't want to clash it with new events, so waiting until it complete the update
      stateActor ? StateActor.UpdateStatus(simulatedTime) map { _ =>
        // Increasing simulation time
        simulatedTime = simulatedTime.plus(1, ChronoUnit.MINUTES)
        log.info(s"Simulated time: $simulatedTime")

        // Starting to process new events
        self ! NextIteration
      }

    case NextIteration =>
      // One iteration is emitting one or none events. If no events was emitted then we're ready to go to next tick

      (currentMovementEvent, currentTurbineEvent) match {
        case (null, null) =>
          killSelf()
        case (m: MovementEvent, null) =>
          // We have no more turbine events, continue emit movements

          if (currentMovementEvent.timestamp.compareTo(simulatedTime) < 0) {
            stateActor ! m
            currentMovementEvent = nextMovementEvent

            nextStage = NextIteration
            stateIncreaseConsumedMovementEvents()
          } else
            nextTick
        case (null, t: TurbineEvent) =>
          // We have no more movement events, continue emit turbine events

          if (currentTurbineEvent.timestamp.compareTo(simulatedTime) < 0) {
            stateActor ! t
            currentTurbineEvent = nextTurbineEvent

            nextStage = NextIteration
            stateIncreaseConsumedTurbineEvents()
          } else
            nextTick
        case (m, t) =>
          // Earliest event goes to be emitted if it's before current simulated date
          // If both of dates are later than simulated date then we're going to next tick

          if (m.timestamp.compareTo(t.timestamp) < 0) {
            if (m.timestamp.compareTo(simulatedTime) < 0) {
              stateActor ! m
              currentMovementEvent = nextMovementEvent

              nextStage = NextIteration
              stateIncreaseConsumedMovementEvents()
            } else nextTick
          } else {
            if (t.timestamp.compareTo(simulatedTime) < 0) {
              stateActor ! t
              currentTurbineEvent = nextTurbineEvent

              nextStage = NextIteration
              stateIncreaseConsumedTurbineEvents()
            } else nextTick
          }
      }

    case SaveSnapshotSuccess(_) =>
      // This is because we want to make sure that current state was saved before moving to next stage
      if (nextStage != null) {
        self ! nextStage
        nextStage = null
      }

    case KillSelf =>
      log.info("Committing a suicide")
      self ! PoisonPill
  }

  def killSelf(): Unit = {
    log.info("No events to process")

    // Asking child actor to make a cleanup
    stateActor ! StateActor.Cleanup

    // Cleaning the state and going to exit
    nextStage = KillSelf
    stateClear()
  }

  def stateClear(): Unit = {
    log.info("Clearing the state")
    state = ActorState()
    saveSnapshot(state)
  }

  def stateUpdateSimulatedTime(): Unit = {
    state = ActorState(
      state.consumedMovementEvents,
      state.consumedTurbineEvents,
      simulatedTime
    )
    saveSnapshot(state)
  }

  def stateIncreaseConsumedMovementEvents(): Unit = {
    state = ActorState(
      state.consumedMovementEvents + 1,
      state.consumedTurbineEvents,
      state.simulatedTime
    )
    saveSnapshot(state)
  }

  def stateIncreaseConsumedTurbineEvents(): Unit = {
    state = ActorState(
      state.consumedMovementEvents,
      state.consumedTurbineEvents + 1,
      state.simulatedTime
    )
    saveSnapshot(state)
  }

  def nextMovementEvent: MovementEvent =
    if (movementEvents.hasNext) movementEvents.next() else null
  def nextTurbineEvent: TurbineEvent =
    if (turbineEvents.hasNext) turbineEvents.next() else null
}

object EventEmitterActor {
  abstract class ActorStage

  case object Start extends ActorStage
  case object Tick extends ActorStage
  case object NextIteration extends ActorStage
  case object KillSelf extends ActorStage
  case object UpdateChildStatus extends ActorStage

  case class ActorState(consumedMovementEvents: Int = 0,
                        consumedTurbineEvents: Int = 0,
                        simulatedTime: Instant = null)
}
