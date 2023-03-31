package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CharactersConstants._
import com.bbva.datioamproduct.fdevdatio.common.fields.{RowNumber, SpicyLevel, SpicyTolerance}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import com.bbva.datioamproduct.fdevdatio.common.fields.{Genre, RowNumber, SpicyTolerance}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.GenderConstants.{Female, Male}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.LettersConstants._
import org.apache.spark.sql.functions.{col, concat_ws, when}
import com.bbva.datioamproduct.fdevdatio.common.fields._

package object transformations {

  case class ReplaceColumnException(message: String, columnName: String, columns: Array[String]) extends Exception(message)

  implicit class CustomTransmormations(df:DataFrame) {

    def spicyFilter(food: DataFrame): DataFrame = {
      //Método que filtra los platillos que no tengan los niveles de picor de la secuencia
      //Secuencia: spicyLevelsToFilter
      val spicyLevelsToFilter = Seq("1", "2", "3", "5", "6", "7", "8", "9")
      //spicyLevelsToFilter:_* se utiliza para desempaquetar una secuencia en sus elementos individuales.
      food.filter(!col(SpicyLevel.name).isin(spicyLevelsToFilter: _*))
    }

    def justWomen(foodie: DataFrame): DataFrame = {
      //Devuelve dos registros por nacionalidad que contengan las mujeres que consumen más picante
      foodie.replaceColumn(SpicyTolerance.column) //Remplaza los valores a enteros
        .addColumn(RowNumber())
        .filter(Genre.column === Female && RowNumber.column <= 2)
    }

    def listOfFood(spicyFilter: DataFrame): String = {
      spicyFilter
        .select(concat_ws(TwoPoints, Code.column, Name.column, when(ContainsAnimalProducts.column === N, Asterisk).otherwise(NoSpace)))
        .collect //No usar collect
        .mkString(CommaSpace)
        .replace(TwoPointsParentheses, OpenParenthesis)
        .replaceAll(Brackets, NoSpace)
        .replaceAll(TwoPointsComma, Comma)
    }

    def concatFood(foodie: DataFrame, listOfFood:String): DataFrame = {
      foodie.addColumn(FoodList(listOfFood))
    }

    @throws[Exception]
    def addColumn(newColumn: Column): DataFrame = {
      try {
        val columns: Array[Column] = df.columns.map(col) :+ newColumn
        df.select(columns: _*)
      } catch {
        case exception: Exception => throw exception
      }
    }

    @throws[ReplaceColumnException]
    def replaceColumn(field: Column): DataFrame = { //Este método remplaza los valores string de una columna a enteros
      val columnName: String = field.expr.asInstanceOf[NamedExpression].name

      if (df.columns.contains(columnName)) {
        val columns: Array[Column] = df.columns.map {
          case name: String if name == columnName => field.cast(Integ4pe).alias(name)
          case _@name => col(name)
        }
        df.select(columns: _*)
      } else {
        val message: String = s"La columna $columnName, no puede ser remplazada"
        throw new ReplaceColumnException(message, columnName, df.columns)
      }
    }


  }

}
