package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.FoodConfigConstants.{tkditFoodTag, tkditFoodieTag}
import com.bbva.datioamproduct.fdevdatio.testUtils.{ContextProvider, FakeRuntimeContext}
import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.bbva.datioamproduct.fdevdatio.transformations._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}

class FoodSparkProcessTest extends ContextProvider with IOUtils{

  val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(config)

  "when i execute spicyFilter" should "return spicy levels without ('1','2','3', '5','6','7','8','9')" in {

    val foodDF: DataFrame = read(config.getConfig(tkditFoodTag))

    val listFood: DataFrame = foodDF
      .spicyFilter(foodDF) // 1.- filter by spicy level
    //.show()
    val seqSpicy = Seq("1", "2", "3", "5", "6", "7", "8", "9")
    val countListFood = listFood.select(col("SPICY_LEVEL")).filter(col("SPICY_LEVEL").isin(seqSpicy: _*)).count()

    countListFood.shouldBe(0)

  }

  "when i execute justWomen" should "return maximum two women's by nationality" in {
    val foodieDF: DataFrame = read(config.getConfig(tkditFoodieTag))
    val justWomen = foodieDF
      .justWomen(foodieDF)

    val countJustWomen = justWomen
      .groupBy(col("GENRE"), col("NATIONALITY"))
      .agg(count("*") alias "count")
      .filter(col("GENRE") =!= "FEMALE" || col("count") > 2)
      .count()
    countJustWomen.shouldBe(0)
  }

  "When I execute runProcessMethod with a correct RuntimeContext" should "return zero" in {
    val foodSparkProcess: FoodSparkProcess = new FoodSparkProcess()

    val exitCode: Int = foodSparkProcess.runProcess(fakeRuntimeContext)

    exitCode shouldBe 0
  }

  it should "return exit code -1 when AnalysisException is launched" in {
    val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(configAnalysisException)
    val courseSparkProcess: FoodSparkProcess = new FoodSparkProcess()

    val exitCode: Int = (courseSparkProcess.runProcess(fakeRuntimeContext))

    exitCode shouldBe -1

  }
/*
  it should "return exit code -1 when InvalidDatasetException is launched" in {
    val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(configInvalidDatasetException)
    val courseSparkProcess: FoodSparkProcess = new FoodSparkProcess()

    val exitCode: Int = (courseSparkProcess.runProcess(fakeRuntimeContext))

    exitCode shouldBe -1

  }
*/
}

