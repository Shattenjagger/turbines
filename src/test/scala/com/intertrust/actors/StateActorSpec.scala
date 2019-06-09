package com.intertrust.actors

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.persistence.inmemory.extension.{
  InMemoryJournalStorage,
  InMemorySnapshotStorage,
  StorageExtension
}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.intertrust.protocol._
import org.scalatest.{
  BeforeAndAfterAll,
  BeforeAndAfterEach,
  Matchers,
  WordSpecLike
}

import scala.concurrent.duration._

class StateActorSpec
    extends TestKit(ActorSystem("StateActorSpec"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  def buildActor(probe: TestProbe): ActorRef =
    system.actorOf(Props(new StateActor() {
      override def getAlertsActor: ActorRef = probe.ref
    }))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    val tp = TestProbe()
    tp.send(
      StorageExtension(system).journalStorage,
      InMemoryJournalStorage.ClearJournal
    )
    tp.expectMsg(akka.actor.Status.Success(""))
    tp.send(
      StorageExtension(system).snapshotStorage,
      InMemorySnapshotStorage.ClearSnapshots
    )
    tp.expectMsg(akka.actor.Status.Success(""))
    super.beforeEach()
  }

  "StateActor" must {
    "raise turbine alert on state change" in {
      val probe = TestProbe()
      val actor = buildActor(probe)
      actor ! TurbineEvent("X", TurbineStatus.Working, 0.0, Instant.now())
      actor ! TurbineEvent("X", TurbineStatus.Broken, 0.0, Instant.now())
      probe.expectMsgAllClassOf(10.seconds, classOf[TurbineAlert])
      probe.expectNoMessage(1.seconds)
    }

    "raise turbine alert if no persons come in 4 hours" in {
      val probe = TestProbe()
      val actor = buildActor(probe)
      actor ! TurbineEvent("X", TurbineStatus.Working, 0.0, Instant.now())
      actor ! TurbineEvent("X", TurbineStatus.Broken, 0.0, Instant.now())
      probe.expectMsgAllClassOf(10.seconds, classOf[TurbineAlert])
      probe.expectNoMessage(1.seconds)

      actor ! StateActor.UpdateStatus(Instant.now().plus(4, ChronoUnit.HOURS))
      probe.expectMsgAllClassOf(10.seconds, classOf[TurbineAlert])
      probe.expectNoMessage(1.seconds)
    }

    "raise turbine alert if turbine still not working after last visit in 3 minutes" in {
      val probe = TestProbe()
      val actor = buildActor(probe)
      actor ! TurbineEvent("X", TurbineStatus.Working, 0.0, Instant.now())
      actor ! TurbineEvent("X", TurbineStatus.Broken, 0.0, Instant.now())
      probe.expectMsgAllClassOf(10.seconds, classOf[TurbineAlert])
      probe.expectNoMessage(1.seconds)

      actor ! MovementEvent("P1", Turbine("X"), Movement.Enter, Instant.now())
      actor ! MovementEvent("P1", Turbine("X"), Movement.Exit, Instant.now())
      probe.expectNoMessage(1.seconds)

      actor ! StateActor.UpdateStatus(Instant.now().plus(3, ChronoUnit.MINUTES))
      probe.expectMsgAllClassOf(10.seconds, classOf[TurbineAlert])
      probe.expectNoMessage(1.seconds)
    }

    "recover state" in {
      val probe = TestProbe()
      val actor_1 = buildActor(probe)
      actor_1 ! TurbineEvent("X", TurbineStatus.Working, 0.0, Instant.now())
      actor_1 ! TurbineEvent("X", TurbineStatus.Broken, 0.0, Instant.now())
      probe.expectMsgAllClassOf(10.seconds, classOf[TurbineAlert])
      probe.expectNoMessage(1.seconds)

      actor_1 ! PoisonPill

      val anotherProbe = TestProbe()
      val actor_2 = buildActor(anotherProbe)
      actor_2 ! StateActor.UpdateStatus(Instant.now().plus(4, ChronoUnit.HOURS))
      anotherProbe.expectMsgAllClassOf(10.seconds, classOf[TurbineAlert])
      anotherProbe.expectNoMessage(1.seconds)
    }

    "be silent on normal movement flow" in {
      val probe = TestProbe()
      val actor = buildActor(probe)
      actor ! MovementEvent("P1", Turbine("X"), Movement.Enter, Instant.now())
      actor ! MovementEvent("P1", Turbine("X"), Movement.Exit, Instant.now())
      probe.expectNoMessage(1.seconds)
    }

    "raise movement alert on exit without enter" in {
      val probe = TestProbe()
      val actor = buildActor(probe)
      actor ! MovementEvent("P1", Turbine("X"), Movement.Exit, Instant.now())
      probe.expectMsgAllClassOf(10.seconds, classOf[MovementAlert])
      probe.expectNoMessage(1.seconds)
    }

    "raise movement alert on enter while being in another location" in {
      val probe = TestProbe()
      val actor = buildActor(probe)
      actor ! MovementEvent("P1", Turbine("X"), Movement.Enter, Instant.now())
      actor ! MovementEvent("P1", Vessel("V1"), Movement.Enter, Instant.now())
      probe.expectMsgAllClassOf(10.seconds, classOf[MovementAlert])
      probe.expectNoMessage(1.seconds)
    }
  }
}
