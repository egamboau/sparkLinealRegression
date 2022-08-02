package cenfotec.ambientesdistribuidos

import java.io.BufferedOutputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap


/**
 * Class that performs a basic lineal regression with 2 variables. 
 * 
 * Algorithm was taken from here: http://faculty.cas.usf.edu/mbrannick/regression/Part3/Reg2.html
*/
object LinealRegression{

  def processFile(originPath: String, destinationPath: String, sc: SparkContext): Unit = {
    val csvFile = sc.textFile(originPath)
    val calculatedValues = csvFile.map(s => s.split(","))
      .map{array => {
        //with each of the lines on the CSV, calculate all of the elements that we need, multiplying and squaring as well. We also add an Accumulator for the 
        //average that is needed for the process. The name of the map is just the element, so it can be translated easilly
        val X1 = array(0).toDouble
        val X2 = array(1).toDouble
        val Y = array(2).toDouble
        val X1X1 = Math.pow(X1, 2)
        val X2X2 = Math.pow(X2, 2)
        val X1Y = X1 * Y
        val X2Y = X2 * Y
        val X1X2 = X1 * X2

        HashMap("X1"->X1, "X2"-> X2, "X1Squared"->X1X1, "X2Squared"->X2X2, "X1Y" -> X1Y, "X1X2"-> X1X2, "X2Y"-> X2Y,"Y"-> Y, "Accumulator" -> 1.0)
      }}.aggregate( 
        //Aggregation that will implement all of the sums from the previous values. The addition is made per-key element wise.
        HashMap("X1"->0.0, "X2"-> 0.0, "X1Squared"->0.0, "X2Squared"->0.0, "X1Y" -> 0.0, "X1X2"-> 0.0, "X2Y"-> 0.0, "Y"-> 0.0, "Accumulator" -> 0.0))((acc, value) => {
          for ((key, value ) <- value) {
            acc(key) += value
          }
          acc
        }, //accumulator for each partition. Return the same acc variable, so avoid creating a new object
       (acc, value) => {
        for ((key, value ) <- value) {
            acc(key) += value
          }
          acc
    }) //accumulator accorss partitions

    val sumOfSquaresX1 = calculatedValues("X1Squared") - (Math.pow(calculatedValues("X1"),2) / calculatedValues("Accumulator"))
    val sumOfSquaresX2 = calculatedValues("X2Squared") - (Math.pow(calculatedValues("X2"),2) / calculatedValues("Accumulator"))

    val crossProductX1Y = calculatedValues("X1Y") - ((calculatedValues("X1") * calculatedValues("Y")) / calculatedValues("Accumulator"))
    val crossProductX2Y = calculatedValues("X2Y") - ((calculatedValues("X2") * calculatedValues("Y")) / calculatedValues("Accumulator"))
    val crossProductX1X2 = calculatedValues("X1X2") - ((calculatedValues("X1") * calculatedValues("X2")) / calculatedValues("Accumulator"))

    val dividend = (sumOfSquaresX1 *  sumOfSquaresX2) - Math.pow(crossProductX1X2, 2)
    val A = ((sumOfSquaresX2 * crossProductX1Y) - (crossProductX1X2 * crossProductX2Y)) / dividend
    val B = ((sumOfSquaresX1 * crossProductX2Y) - (crossProductX1X2 * crossProductX1Y)) / dividend
    val M = ((calculatedValues("Y")) / calculatedValues("Accumulator")) - (A * (calculatedValues("X1")/calculatedValues("Accumulator"))) - (B * (calculatedValues("X2")/calculatedValues("Accumulator")))

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val output = fs.create(new Path(destinationPath))
    val os = new BufferedOutputStream(output)
    os.write(s"$M,$A,$B".getBytes())
    os.close()
    println(s"X ~ $M + $A A + $B B")
  }

  
  def main(args: Array[String]): Unit = {
    //initialize spark context
    val conf = new SparkConf(loadDefaults = true)
      .setAppName("linealRegression")

    val sc = new SparkContext(conf)
    //first argument will be the path for the data
    val originPath = args(0)
    val destinationPath = args(1)

    processFile(originPath, destinationPath, sc)

  }
}
