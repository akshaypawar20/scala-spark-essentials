package scala_basics

import scala.util.Random

object ScalaBasics extends App{

  // VALUES and VARIABLES

  val anInteger: Int = 10
  val aCharacter: Char = 'A'
  val aString: String = "Akshay"
  val aFloat: Float = 20.30f
  val aDouble: Double = 20.30
  val aBoolean: Boolean = false

  var aVariable: Int = 10
  aVariable += 10
  println(aVariable)

  // EXPRESSIONS
  val aCondition: Boolean = true
  val aConditionedValue = if (aCondition) 50 else 30
  println(aConditionedValue)


  // CODE BLOCKS

  val aCodeBlockValue = {
    val aNumber = 10
    if (aNumber % 2 == 0) "EVEN" else "ODD"
  }
  println(aCodeBlockValue)

  // FUNCTIONS

  def concat(firstName: String, lastName: String): String = {
    return firstName + " " + lastName
  }

  println("Fullname: " + concat("Akshay", "Pawar"))

  // STRING OPERATIONS
  println("---------- STRING OPERATIONS --------")
  val myString: String = "Hello, I am Akshay Pawar and my age is 26"
  println(myString.charAt(0))
  println(myString.substring(0,5))
  println(myString.replace(" ", "-"))
  println(myString.concat(", Really??"))
  println(myString.contains("A"))
  println(myString.substring(myString.length - 2, myString.length).toInt)
  println(myString.toLowerCase + myString.toUpperCase)
  println(myString.reverse)

  // S INTEPOLERATORS
  val myName = "Akshay Pawar"
  val myAge  = 26
  val myGreeting = s"My name is $myName and my age is $myAge"
  println(myGreeting)

  // FUNCTIONAL PROGRAMMING
  val incrementer: Int => Int = x => x + 1
  val anotherIncrementer: (Int, Int) => Int = (x, y) => x + y
  val incremented = incrementer(20)
  val anotherIncremented = anotherIncrementer(20, 5)
  println(incremented)
  println(anotherIncremented)

  val listIncremnted = List(1,2,3,4,5).map(incrementer)
  println(listIncremnted)

  // PATTERN MATCHING
  val random = new Random
  val aRandomInt: Int = random.nextInt(10)
  println(aRandomInt)

  val description = aRandomInt match {
    case 1 => "FIRST"
    case 2 => "SECOND"
    case 3 => "THIRD"
    case 4 => "FOURTH"
    case 5 => "FIFTH"
    case _ => "OTHER"
  }
  println(description)
}


