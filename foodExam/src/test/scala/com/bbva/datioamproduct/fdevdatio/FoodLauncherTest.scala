package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.datio.dataproc.sdk.launcher.SparkLauncher

class FoodLauncherTest extends ContextProvider{

  "SparLauncher execute" should "return 0 in success execution" in {

    val args: Array[String] = Array(
    "src/test/resources/config/FoodTest.conf", "FoodSparkProcess"
    )

    val exitCode: Int = new SparkLauncher().execute(args)

    exitCode shouldBe 0

  }

  it should "return exit code 1 when wrong configuration values are introduced" in {
    val args: Array[String] = Array(
      "src/test/resources/config/FoodTest.conff", "CourseSparkProcess"
    )
    val exitCode: Int = new SparkLauncher().execute(args)

    exitCode shouldBe 1

  }


}
