package com.bbva.datioamproduct.fdevdatio.common

object StaticVals {
  case object FoodConfigConstants{
    val RootTag:String = "foodJob"
    val InputTag: String = s"$RootTag.input"
    val OutputTag: String = s"$RootTag.output"
    val ParamsTag: String = s"$RootTag.params"

    //CAMPOS EXTERNOS
    val DevNameTag: String = s"$ParamsTag.devName"

    //TABLAS
    val tkditFoodTag: String = s"$InputTag.tkditFood"
    val tkditFoodieTag: String = s"$InputTag.tkditFoodie"
    val FoodTag: String = s"tkditFood"
    val FoodieTag: String = s"tkditFoodie"
  }

  case object GenderConstants{
    val Male : String = "MALE"
    val Female : String = "FEMALE"
  }

  case object NumericConstants{
    val Zero: Int = 0
    val NegativeOne: Int = -1
  }

  case object LettersConstants{
    val N: String= "N"
  }
  case object CharactersConstants{
    val CommaSpace: String = ", "
    val Comma: String = ","
    val TwoPointsComma: String = ":,"
    val TwoPoints: String = ":"
    val Asterisk: String = "(*)"
    val NoSpace: String = ""
    val Brackets:String = "\\[|\\]"
    val TwoPointsParentheses:String = ":("
    val OpenParenthesis: String = "("
  }

  case object Messages {
    val WelcomeMessage: String => String = {
      (devName: String) => s"Examen hecho por: $devName"
    }
  }


}
