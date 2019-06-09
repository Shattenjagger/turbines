package com.intertrust.actors

import java.io.{ByteArrayInputStream, InputStream}
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

class EventEmitterActorSpec
    extends TestKit(ActorSystem("EventEmitterActorSpec"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  def buildActor(probe: TestProbe,
                 movementsStream: InputStream,
                 turbinesStream: InputStream): ActorRef =
    system.actorOf(
      Props(new EventEmitterActor(movementsStream, turbinesStream) {
        override def getStateActor: ActorRef = probe.ref
        override def shutdown(): Unit = ()
      })
    )

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

  "EventEmitterActor" must {
    "emit events in right sequence" in {
      val movementsDataset =
        """Date,Location,Person,Movement type
          |23.11.2015 03:34,E8J,P57,Exit
          |23.11.2015 03:34,Vessel 235090838,P57,Enter
          |23.11.2015 03:35,Vessel 235090838,P19,Exit""".stripMargin
      val turbinesDataset =
        """Date,ID,ActivePower (MW),Status
          |2015-11-23 03:33:00,H7R,3.00,Working
          |2015-11-23 03:33:00,B3A,-0.11,Broken
          |2015-11-23 03:37:00,E7J,3.32,Working""".stripMargin

      val probe = TestProbe()
      val actor = buildActor(
        probe,
        new ByteArrayInputStream(movementsDataset.getBytes),
        new ByteArrayInputStream(turbinesDataset.getBytes)
      )

      actor ! EventEmitterActor.Start

      // 3:33
      probe.expectMsgAllClassOf(10.seconds, classOf[StateActor.UpdateStatus])
      probe.reply(StateActor.UpdateStatusDone)

      probe.expectMsgAllClassOf(
        10.seconds,
        classOf[TurbineEvent],
        classOf[TurbineEvent]
      )

      // 3:34
      probe.expectMsgAllClassOf(10.seconds, classOf[StateActor.UpdateStatus])
      probe.reply(StateActor.UpdateStatusDone)

      probe.expectMsgAllClassOf(
        10.seconds,
        classOf[MovementEvent],
        classOf[MovementEvent]
      )

      // 3:35
      probe.expectMsgAllClassOf(10.seconds, classOf[StateActor.UpdateStatus])
      probe.reply(StateActor.UpdateStatusDone)

      probe.expectMsgAllClassOf(10.seconds, classOf[MovementEvent])

      // 3:36
      probe.expectMsgAllClassOf(10.seconds, classOf[StateActor.UpdateStatus])
      probe.reply(StateActor.UpdateStatusDone)

      // 3:37
      probe.expectMsgAllClassOf(10.seconds, classOf[StateActor.UpdateStatus])
      probe.reply(StateActor.UpdateStatusDone)

      probe.expectMsgAllClassOf(10.seconds, classOf[TurbineEvent])
    }

    "emit movement events normally with empty turbines dataset" in {
      val movementsDataset =
        """Date,Location,Person,Movement type
          |23.11.2015 09:21,E8J,P57,Exit
          |23.11.2015 09:21,Vessel 235090838,P57,Enter
          |23.11.2015 09:21,Vessel 235090838,P19,Exit
          |23.11.2015 09:21,F3P,P19,Enter""".stripMargin
      val turbinesDataset =
        """Date,ID,ActivePower (MW),Status
          |""".stripMargin

      val probe = TestProbe()
      val actor = buildActor(
        probe,
        new ByteArrayInputStream(movementsDataset.getBytes),
        new ByteArrayInputStream(turbinesDataset.getBytes)
      )

      actor ! EventEmitterActor.Start

      probe.expectMsgAllClassOf(10.seconds, classOf[StateActor.UpdateStatus])
      probe.reply(StateActor.UpdateStatusDone)

      probe.expectMsgAllClassOf(
        10.seconds,
        classOf[MovementEvent],
        classOf[MovementEvent],
        classOf[MovementEvent],
        classOf[MovementEvent]
      )
    }

    "emit turbines events normally with empty movements dataset" in {
      val movementsDataset =
        """Date,Location,Person,Movement type
          |""".stripMargin
      val turbinesDataset =
        """Date,ID,ActivePower (MW),Status
          |2015-11-23 03:33:00,H7R,3.00,Working
          |2015-11-23 03:33:00,B3A,-0.11,Broken
          |2015-11-23 03:33:00,E7J,3.32,Working
          |2015-11-23 03:33:00,F10H_PR,3.09,Working""".stripMargin

      val probe = TestProbe()
      val actor = buildActor(
        probe,
        new ByteArrayInputStream(movementsDataset.getBytes),
        new ByteArrayInputStream(turbinesDataset.getBytes)
      )

      actor ! EventEmitterActor.Start

      probe.expectMsgAllClassOf(10.seconds, classOf[StateActor.UpdateStatus])
      probe.reply(StateActor.UpdateStatusDone)

      probe.expectMsgAllClassOf(
        10.seconds,
        classOf[TurbineEvent],
        classOf[TurbineEvent],
        classOf[TurbineEvent],
        classOf[TurbineEvent]
      )
    }

    "restore state" in {
      val movementsDataset =
        """Date,Location,Person,Movement type
            |23.11.2015 03:34,E8J,P57,Exit""".stripMargin
      val turbinesDataset =
        """Date,ID,ActivePower (MW),Status
            |2015-11-23 03:33:00,H7R,3.00,Working""".stripMargin

      val probe = TestProbe()
      val actor = buildActor(
        probe,
        new ByteArrayInputStream(movementsDataset.getBytes),
        new ByteArrayInputStream(turbinesDataset.getBytes)
      )

      actor ! EventEmitterActor.Start

      // 3:33
      probe.expectMsgAllClassOf(10.seconds, classOf[StateActor.UpdateStatus])
      probe.reply(StateActor.UpdateStatusDone)

      probe.expectMsgAllClassOf(10.seconds, classOf[TurbineEvent])

      // 3:34
      probe.expectMsgAllClassOf(10.seconds, classOf[StateActor.UpdateStatus])

      actor ! PoisonPill

      val anotherProbe = TestProbe()
      val actor_2 = buildActor(
        anotherProbe,
        new ByteArrayInputStream(movementsDataset.getBytes),
        new ByteArrayInputStream(turbinesDataset.getBytes)
      )

      actor_2 ! EventEmitterActor.Start

      // 3:34
      anotherProbe.expectMsgAllClassOf(
        10.seconds,
        classOf[StateActor.UpdateStatus]
      )
      anotherProbe.reply(StateActor.UpdateStatusDone)

      anotherProbe.expectMsgAllClassOf(10.seconds, classOf[MovementEvent])
    }
  }
}
