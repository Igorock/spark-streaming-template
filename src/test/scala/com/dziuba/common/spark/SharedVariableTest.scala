package com.dziuba.common.spark

import com.dziuba.common.util.TestConfig

class SharedVariableTest extends TestConfig {

  it should "create shared variable" in {
    val sharedVariable = SharedVariable {
      "test"
    }

    assertResult("test")(sharedVariable.get)
  }

}
