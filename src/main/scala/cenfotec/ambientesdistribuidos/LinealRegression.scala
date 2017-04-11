package cenfotec.ambientesdistribuidos

import org.apache.spark.{SparkConf, SparkContext}

object LinealRegression{

  def processFile(filePath: String, sc: SparkContext): Unit = {
    val csvFile = sc.textFile(filePath)
    val calculatedValues = csvFile.map(s => s.split(","))
      .map(array => {
        val X1 = array(0).toDouble
        val X2 = array(1).toDouble
        val Y = array(2).toDouble
        val X1X1 = Math.pow(X1, 2)
        val X2X2 = Math.pow(X2, 2)
        val X1Y = X1 * Y
        val X2Y = X2 * Y
        val X1X2 = X1 * X2
        (X1, X2, X1X1, X2X2, X1Y, X2Y, X1X2, Y, 1)
      }).aggregate((0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0))((acc, value) => {
      (acc._1 + value._1,
        acc._2 + value._2,
        acc._3 + value._3,
        acc._4 + value._4,
        acc._5 + value._5,
        acc._6 + value._6,
        acc._7 + value._7,
        acc._8 + value._8,
        acc._9 + value._9)
    }, (acc, value) => {
      (acc._1 + value._1,
        acc._2 + value._2,
        acc._3 + value._3,
        acc._4 + value._4,
        acc._5 + value._5,
        acc._6 + value._6,
        acc._7 + value._7,
        acc._8 + value._8,
        acc._9 + value._9)
    })
    val dividend = (calculatedValues._3 *  calculatedValues._4) - Math.pow(calculatedValues._8, 2)
    val A = ((calculatedValues._4 * calculatedValues._5) - (calculatedValues._7 * calculatedValues._6)) / dividend
    val B = ((calculatedValues._3 * calculatedValues._6) - (calculatedValues._7 * calculatedValues._5)) / dividend
    val M= (calculatedValues._8 / calculatedValues._9) - (A * (calculatedValues._1/calculatedValues._9)) - (B * (calculatedValues._2 / calculatedValues._9))

    println(s"X ~ $M + $A A + $B B")
  }

  def main(args: Array[String]): Unit = {
    //initialize spark context
    val conf = new SparkConf()
      .setAppName("linealRegression")
      .setMaster("local[2]") //remove this for final delivery. This is only for IntelliJ
    val sc = new SparkContext(conf)
    //first argument will be the path for the data
    val filePath = args(0)

    processFile(filePath, sc)

  }





}
