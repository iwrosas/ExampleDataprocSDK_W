package com.bbva.datioamproduct.fdevdatio.common

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, count, lit, row_number}


package object fields {
  case object SpicyLevel extends Field {
    override val name: String = "SPICY_LEVEL"
  }

  case object SpicyTolerance extends Field {
    override val name: String = "SPICY_TOLERANCE"
  }

  case object Genre extends Field {
    override val name: String = "GENRE"
  }

  case object Nationality extends Field {
    override val name: String = "NATIONALITY"
  }

  case object Code extends Field {
    override val name: String = "CODE"
  }

  case object Name extends Field {
    override val name: String = "NAME"
  }

  case object ContainsAnimalProducts extends Field {
    override val name: String = "CONTAINS_ANIMAL_PRODUCTS"
  }

   case object RowNumber extends Field {
     override val name: String = "ROW_NUMBER"
     def apply(): Column = {
       val w = Window.partitionBy(Nationality.name, Genre.name).orderBy(SpicyTolerance.column.desc)
       row_number() over w alias name
     }
  }

  case object FoodList extends Field{
    override val name: String = "FOOD_LIST"
    def apply(listOfFood: String): Column = lit(listOfFood) alias name
  }


}
