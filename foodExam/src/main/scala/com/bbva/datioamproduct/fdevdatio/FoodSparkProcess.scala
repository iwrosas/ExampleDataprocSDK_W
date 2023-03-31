package com.bbva.datioamproduct.fdevdatio
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.FoodConfigConstants.{FoodTag, FoodieTag, tkditFoodTag, tkditFoodieTag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.Messages.WelcomeMessage
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.NumericConstants.{NegativeOne, Zero}
import com.bbva.datioamproduct.fdevdatio.utils.{IOUtils, Params, SuperConfig}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.Config
import com.bbva.datioamproduct.fdevdatio.transformations.{CustomTransmormations, ReplaceColumnException}
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

class FoodSparkProcess extends SparkProcess with IOUtils{

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def runProcess(runtimeContext: RuntimeContext): Int = {


    Try {

      val config: Config = runtimeContext.getConfig
      val params: Params = config.getParams
      logger.info(WelcomeMessage(params.devName))
      val dataFrameReader: Map[String, DataFrame] = config.readInputs

      //Paso 1
      val foodDF: DataFrame = dataFrameReader(FoodTag)
      val spicyFilterDF: DataFrame = foodDF.spicyFilter(foodDF)
      spicyFilterDF.show()

      //Paso 2
      val foodieDF: DataFrame = dataFrameReader(FoodieTag)
      val justWomenDF: DataFrame = foodieDF.justWomen(foodieDF)
      justWomenDF.show()

      //Paso 3
      val listOfFood: String = foodDF.listOfFood(spicyFilterDF)
      println(listOfFood)

      //Paso 4
      val concatFoodDF: DataFrame = justWomenDF.concatFood(justWomenDF, listOfFood)
      concatFoodDF.show(false)
      concatFoodDF.printSchema()

    } match {
      case Success(_) => Zero
      case Failure(exception: ReplaceColumnException) =>
        logger.error(exception.getMessage)
        logger.error(s"Columna a reemplazar: ${exception.columnName}")
        logger.error(s"Columnas encontradas: [${exception.columns.mkString(", ")}]")
        NegativeOne
      case Failure(exception: InvalidDatasetException) =>
        exception.getErrors.forEach { error =>
          logger.error(error.toString)
        }
        NegativeOne
      case Failure(exception: Exception) =>
        exception.printStackTrace()
        NegativeOne
    }
  }
  override def getProcessId: String = "FoodSparkProcess"

}
