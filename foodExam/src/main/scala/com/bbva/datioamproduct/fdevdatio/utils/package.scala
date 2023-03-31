package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.FoodConfigConstants.{DevNameTag, InputTag}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import scala.collection.convert.ImplicitConversions._

package object utils {

  case class Params(devName: String)
  implicit class SuperConfig(config: Config) extends IOUtils {
    def readInputs(): Map[String, DataFrame] = {
      config.getObject(InputTag).keySet()
        .map(key => {
          val inputConfig: Config = config.getConfig(s"$InputTag.$key")
          (key, read(inputConfig))
        }).toMap
    }

    def getParams: Params = Params(
      devName = config.getString(DevNameTag)
    )

  }
}

