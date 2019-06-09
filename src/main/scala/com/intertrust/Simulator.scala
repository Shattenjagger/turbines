package com.intertrust

import java.io.InputStream

import akka.actor.{ActorSystem, Props}
import com.intertrust.actors.EventEmitterActor

object Simulator {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("simulator")

    val movementsStream: InputStream =
      getClass.getResourceAsStream("/movements.csv")
    val turbinesStream: InputStream =
      getClass.getResourceAsStream("/turbines.csv")

    val eventEmitterActor =
      system.actorOf(
        Props(new EventEmitterActor(movementsStream, turbinesStream)),
        "eventEmitterActor"
      )

    eventEmitterActor ! EventEmitterActor.Start
  }
}
