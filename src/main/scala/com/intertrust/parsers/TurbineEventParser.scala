package com.intertrust.parsers

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import com.intertrust.parsers.TurbineEventParser.timestampFormat
import com.intertrust.protocol.{TurbineEvent, TurbineStatus}

class TurbineEventParser extends EventParser[TurbineEvent] {
  override protected def parseEvent(columns: Array[String]): TurbineEvent = {
    TurbineEvent(
      turbineId = columns(1),
      status = TurbineStatus.withName(columns(3)),
      generation = columns(2).toDouble,
      timestamp = Instant.from(timestampFormat.parse(columns(0)))
    )
  }
}

object TurbineEventParser {
  private val timestampFormat = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:mm:ss")
    .withZone(ZoneId.of("UTC"))
}
