package com.dziuba.common.metrics

import java.util.Calendar

import com.typesafe.scalalogging.Logger

object BenchmarkHelper {


  implicit class Marked(logger: Logger) {

    def printBenchmark(benchmarkProperties: BenchmarkProperties) {
      val endTime = Calendar.getInstance().getTimeInMillis
      val timeTaken = endTime - benchmarkProperties.startTime
      logger.info(s"Total time taken for $benchmarkProperties: $timeTaken")
    }
  }

}
