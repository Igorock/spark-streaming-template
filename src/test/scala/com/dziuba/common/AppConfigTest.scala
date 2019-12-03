package com.dziuba.common

import com.dziuba.common.util.TestConfig

class AppConfigTest extends TestConfig {

  it should "test InputArgs initialization" in {
    val config = AppConfig()
    assertResult(config.IncomeTopicPrefix)("PERSISTENCE_")
    assertResult(config.ExportTopicPrefix)("EXPORT_")
  }

  it should "work with partly set properties" in {
    val config = AppConfig(
      "app.persistence.topic-tables" -> "",
      "spark.processing-time" -> "1000")


    assertResult(1000)(config.ProcessingTime)
    assertResult(0)(config.TopicTables.size)
  }

  it should "successfully pass w/ topic-tables property set" in {
    val config = AppConfig(
      "app.persistence.topic-tables" -> "TABLE1,TABLE2 ,TABLE3")

    assertResult(3)(config.TopicTables.size)
  }
}
