package cenfotec.ambientesdistribuidos

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap

/**
  * Created by vagrant on 4/12/17.
  */
object ModelEvaluation {


  def processFiles(modelPath: String, testDataSetPath: String, sc: SparkContext): Unit = {
    //get the model. we are sure that we have only one, so extract here.
    val model = sc.textFile(modelPath)
      .map(s => {
        val splitted = s.split(",")
        val M = splitted(0).toDouble
        val A = splitted(1).toDouble
        val B = splitted(2).toDouble
        HashMap("M"->M, "A"->A, "B"->B)
      })
    .first()

    val testDataSet = sc.textFile(testDataSetPath).map(s => s.split(","))
    val estimatesAndObserved = testDataSet.map(array =>{
      val observed = array(2).toDouble
      val A = array(0).toDouble
      val B = array(1).toDouble

      //using A and B, estimate the value
      val estimate = model("M") + model("A")*A + model("B")*B
      //return the difference, the observed, and a counter.
      HashMap("EstimateMinusObserved" -> (estimate - observed) , "Observed" -> observed,"Count" -> 1.0, "ObservedMinusStimate" -> (observed - estimate))
    }).persist()

    //calculate the RMSE. Based on http://www.statisticshowto.com/rmse/
    val RMSE_values = estimatesAndObserved.aggregate(HashMap("Residuals" -> 0.0,"Count" -> 0.0))(
      (acc, value) => {
        acc("Residuals") += Math.pow(value("EstimateMinusObserved"), 2)
        acc("Count") += value("Count")
        acc
      },
      (acc, value) => {
        for ((key, value ) <- value) {
            acc(key) += value
          }
        acc
      })

    val RMSE = Math.sqrt(RMSE_values("Residuals") / RMSE_values("Count"))
    System.out.println(s"RMSE is $RMSE")
    //calculate MAE, based on https://en.wikipedia.org/wiki/Mean_absolute_error
     val MAE_values = estimatesAndObserved.aggregate(HashMap("Errors" -> 0.0,"Count" ->0.0))(
       (acc, value) => {
        acc("Errors") += Math.pow(value("EstimateMinusObserved"), 2)
        acc("Count") += value("Count")
        acc
       },
       (acc, value) => {
        for ((key, value ) <- value) {
            acc(key) += value
          }
        acc
      }
     )
    val MAE = MAE_values("Errors") / MAE_values("Count")
    System.out.println(s"MAE is $MAE")

    //calculate MAPE, based on https://en.wikipedia.org/wiki/Mean_absolute_percentage_error
    val MAPE_Values = estimatesAndObserved.aggregate(HashMap("Difference"-> 0.0, "Count" -> 0.0)) (
      (acc, value) =>  {
        val difference = value("ObservedMinusStimate")
        val absoluteValue = Math.abs(difference / value("Observed"))
        acc("Difference") += absoluteValue
        acc("Count") += value("Count")
        acc
      },
      (acc,value) => {
        for ((key, value ) <- value) {
            acc(key) += value
          }
        acc
      }
    )
    val MAPE = (100/MAPE_Values("Count")) * MAPE_Values("Difference")
    System.out.println(s"MAPE is $MAPE")
  }


  def main(args:Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("modelEvaluation")

    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    //first argument will be the path for the data
    val modelPath = args(0)
    val testDataSetPath = args(1)
    processFiles(modelPath, testDataSetPath, sc)
  }
}
