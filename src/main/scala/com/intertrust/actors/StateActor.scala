package com.intertrust.actors

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.persistence.{PersistentActor, SnapshotOffer}
import com.intertrust.protocol._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class StateActor extends PersistentActor with ActorLogging {
  import StateActor._

  override def persistenceId = "state-actor"

  val alertsActor: ActorRef =
    context.system.actorOf(Props(classOf[AlertsActor]), "alerts")

  var state = ActorState()

  override def receiveCommand: Receive = {
    case t: UpdateStatus =>
      log.debug(s"Got update status message $t")
      processStatusUpdate(t.timestamp)
      sender() ! UpdateStatusDone
    case a: TurbineEvent =>
      log.debug(s"Got turbine event $a")
      processTurbineEvent(a)
    case a: MovementEvent =>
      log.debug(s"Got movement event $a")
      processMovementEvent(a)
    case Cleanup =>
      // Parent actor asked to make cleanup
      log.info("Clearing the state")
      state = ActorState()
      saveState()
  }

  def receiveRecover: Receive = {
    case SnapshotOffer(_, snapshot: ActorState) =>
      log.info(s"Got recover $snapshot")
      state = snapshot
    case s => log.info(s"Received state event $s")
  }

  def processStatusUpdate(timestamp: Instant): Unit = {
    val filteredTurbines: mutable.Map[String, TurbineState] = state.turbinesMap
      .filter {
        case (_, v) =>
          v.status == TurbineStatus.Broken && !v.secondNotificationSent
      }

    for ((_, t) <- filteredTurbines) {
      if (t.lastEngineerExit.compareTo(t.lastBroken) > 0) {
        // Engineers had visited the turbines after the break
        if (t.engineers.isEmpty) {
          // We have no engineers there right now
          if (timestamp
                .minus(3, ChronoUnit.MINUTES)
                .compareTo(t.lastEngineerExit) > 0) {
            // Last engineer had exited more than 3 minutes ago?
            // Fire the alarm!

            // We don't want to send this update each time.
            // We will fire it again if engineers will came to turbine and leave it in broken state
            // That's why we're keeping this flag here
            sendTurbineStillBrokenAfterVisitAlert(t.turbineId, timestamp)
            state.turbinesMap.update(
              t.turbineId,
              TurbineState(
                t.turbineId,
                t.status,
                t.lastBroken,
                t.lastEngineerExit,
                t.engineers,
                secondNotificationSent = true
              )
            )
          }
        }
      } else {
        if (timestamp
              .minus(4, ChronoUnit.HOURS)
              .compareTo(t.lastBroken) > 0) {
          // Here was no engineers after the turbine had broken, and there more than 4 hours passed
          sendTurbineBrokenMoreThanFourHoursAlert(t.turbineId, timestamp)
          state.turbinesMap.update(
            t.turbineId,
            TurbineState(
              t.turbineId,
              t.status,
              t.lastBroken,
              t.lastEngineerExit,
              t.engineers,
              secondNotificationSent = true
            )
          )
        }
      }
    }

    saveState()
  }

  def processTurbineEvent(a: TurbineEvent): Unit = {
    if (state.turbinesMap.contains(a.turbineId)) {
      // We already have this turbine in our state, so basically need to update turbine status

      if (state
            .turbinesMap(a.turbineId)
            .status == TurbineStatus.Working && a.status == TurbineStatus.Broken) {
        // Status was switched from working to broken after this event, so we need to send an alert

        sendTurbineEnteredBrokenStateAlert(a.turbineId, a.timestamp)
        state.turbinesMap(a.turbineId) = TurbineState(
          a.turbineId,
          a.status,
          a.timestamp,
          Instant.MIN,
          state.turbinesMap(a.turbineId).engineers
        )
      } else {
        // We don't care about other turbine state transitions
        state.turbinesMap(a.turbineId) = TurbineState(
          a.turbineId,
          a.status,
          state.turbinesMap(a.turbineId).lastBroken,
          state.turbinesMap(a.turbineId).lastEngineerExit,
          state.turbinesMap(a.turbineId).engineers,
          state.turbinesMap(a.turbineId).secondNotificationSent
        )
      }
    } else {
      log.debug(
        s"Got an event for unrecognized turbine ${a.turbineId}. Creating new entry"
      )
      if (a.status == TurbineStatus.Broken) {
        // The first event status is broken for this turbine, so we need to send an alert

        sendTurbineEnteredBrokenStateAlert(a.turbineId, a.timestamp)
        state.turbinesMap(a.turbineId) = TurbineState(
          a.turbineId,
          a.status,
          a.timestamp,
          Instant.MIN,
          ListBuffer[String]()
        )
      } else
        state.turbinesMap(a.turbineId) = TurbineState(
          a.turbineId,
          a.status,
          null,
          Instant.MIN,
          ListBuffer[String]()
        )
    }

    saveState()
  }

  def processMovementEvent(a: MovementEvent): Unit = {
    if (state.personsMap.contains(a.engineerId)) {
      val currentPersonState = state.personsMap(a.engineerId)
      if (a.movement == Movement.Exit) {
        if (currentPersonState.location != a.location) {
          // Exiting from some location while supposed to be in another one. Need to send an alert
          sendMovementExitWithoutEnterAlert(a)
        }
        updateEngineerExit(a)
      } else {
        if (currentPersonState.location != null) {
          // Entering to another location while being in another one
          sendMovementEnterWithoutExitAlert(a)
        }
        updateEngineerEnter(a)
      }
    } else {
      log.debug(
        s"Got an event for unrecognized engineer ${a.engineerId}. Creating new entry"
      )
      if (a.movement == Movement.Exit) {
        // If we're starting with exit movement then we're in wrong state. Need to raise an alert
        sendMovementExitWithoutEnterAlert(a)
        updateEngineerExit(a)
      } else {
        updateEngineerEnter(a)
      }
    }

    saveState()
  }

  def updateEngineerEnter(a: MovementEvent): Unit = {
    assert(a.movement == Movement.Enter)
    if (a.location.isInstanceOf[Turbine])
      updateTurbineEngineerList(a)
    state.personsMap(a.engineerId) = PersonState(a.engineerId, a.location)
  }

  def updateEngineerExit(a: MovementEvent): Unit = {
    assert(a.movement == Movement.Exit)
    if (a.location.isInstanceOf[Turbine])
      updateTurbineEngineerList(a)
    state.personsMap(a.engineerId) = PersonState(a.engineerId, null)
  }

  def updateTurbineEngineerList(a: MovementEvent): Unit = {
    assert(a.location.isInstanceOf[Turbine])
    val location: Turbine = a.location.asInstanceOf[Turbine]

    if (state.turbinesMap.contains(location.id)) {
      // Going to update engineers list and if last one had exited then update last exited state
      val currentTurbineState = state.turbinesMap(location.id)
      val engineersList =
        if (a.movement == Movement.Exit)
          currentTurbineState.engineers -= a.engineerId
        else currentTurbineState.engineers += a.engineerId
      val (lastEngineerExit, secondNotificationSent) =
        if (engineersList.isEmpty)
          (a.timestamp, false)
        else
          (
            currentTurbineState.lastEngineerExit,
            currentTurbineState.secondNotificationSent
          )

      state.turbinesMap(location.id) = TurbineState(
        currentTurbineState.turbineId,
        currentTurbineState.status,
        currentTurbineState.lastBroken,
        lastEngineerExit,
        engineersList,
        secondNotificationSent
      )
    }
  }

  def saveState(): Unit = {
    saveSnapshot(state)
  }

  def sendTurbineEnteredBrokenStateAlert(turbineId: String,
                                         timestamp: Instant): Unit =
    sendTurbineAlert(turbineId, timestamp, "Turbine entered broken state")

  def sendTurbineStillBrokenAfterVisitAlert(turbineId: String,
                                            timestamp: Instant): Unit =
    sendTurbineAlert(
      turbineId,
      timestamp,
      "Turbine still broken state after engineer visit"
    )

  def sendTurbineBrokenMoreThanFourHoursAlert(turbineId: String,
                                              timestamp: Instant): Unit =
    sendTurbineAlert(
      turbineId,
      timestamp,
      "Turbine is broken more than four hours and there is no engineers"
    )

  def sendTurbineAlert(turbineId: String,
                       timestamp: Instant,
                       msg: String): Unit =
    alertsActor ! TurbineAlert(timestamp, turbineId, msg)

  def sendMovementExitWithoutEnterAlert(a: MovementEvent): Unit =
    sendMovementAlert(a, s"Exit without enter $a")

  def sendMovementEnterWithoutExitAlert(a: MovementEvent): Unit =
    sendMovementAlert(a, s"Enter without exit $a")

  def sendMovementAlert(a: MovementEvent, msg: String): Unit =
    alertsActor ! MovementAlert(a.timestamp, a.engineerId, msg)
}

object StateActor {
  case class ActorState(turbinesMap: mutable.HashMap[String, TurbineState] =
                          mutable.HashMap.empty[String, TurbineState],
                        personsMap: mutable.HashMap[String, PersonState] =
                          mutable.HashMap.empty[String, PersonState])

  case class TurbineState(turbineId: String,
                          status: TurbineStatus,
                          lastBroken: Instant,
                          lastEngineerExit: Instant,
                          engineers: mutable.ListBuffer[String],
                          secondNotificationSent: Boolean = false)
  case class PersonState(engineerId: String, location: Location)

  case class UpdateStatus(timestamp: Instant)

  case object UpdateStatusDone
  case object Cleanup
}
