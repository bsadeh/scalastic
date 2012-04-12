package com.traackr.util

import org.joda.time._, format._

object Timer {
  val formatter = new PeriodFormatterBuilder()
    .appendDays()
    .appendSuffix(" day", " days")
    .appendSeparator(", ")
    .appendHours()
    .appendSuffix(" hour", " hours")
    .appendSeparator(", ")
    .appendMinutes()
    .appendSuffix(" minute", " minutes")
    .appendSeparator(", ")
    .appendSeconds()
    .appendSuffix(" second ", " seconds ")
    .appendSeparator(", ")
    .appendMillis()
    .appendSuffix(" millisecond", " milliseconds")
    .toFormatter

  def time[T](description: String)(block: => T) = {
    val start_time = System.nanoTime()
    try {
      block
    } finally {
      val howManyNanoseconds: Long = System.nanoTime() - start_time
      val period = new Period(howManyNanoseconds / 1000000);
      println("%s [in: %s]".format(description, formatter.print(period)))
    }
  }

}
