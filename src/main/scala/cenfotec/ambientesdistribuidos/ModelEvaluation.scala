package cenfotec.ambientesdistribuidos

import org.apache.spark.{SparkConf, SparkContext}

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
        (M, A, B)
      })
    .first()

    val testDataSet = sc.textFile(testDataSetPath).map(s => s.split(","))
    val estimatesAndObserved = testDataSet.map(array =>{
      val observed = array(0).toDouble
      val A = array(1).toDouble
      val B = array(2).toDouble

      //using A and B, estimate the value
      val estimate = model._1 + model._2*A + model._3*B
      //return the difference, the observed, and a counter.
      (estimate - observed , observed, 1, observed - estimate)
    }).persist()

    //calculate the RMSE. Based on http://www.statisticshowto.com/rmse/
    val RMSE_values = estimatesAndObserved.aggregate((0.0,0.0))(
      (acc, value) => (acc._1 + Math.pow(value._1, 2), acc._2 + value._3),
      (acc, value) => (acc._1 + value._1, acc._2 + value._2))

    val RMSE = Math.sqrt(RMSE_values._1 / RMSE_values._2)
    System.out.println(s"RMSE is $RMSE")
    //calculate MAE, based on https://en.wikipedia.org/wiki/Mean_absolute_error
     val MAE_values = estimatesAndObserved.aggregate((0.0,0.0))(
       (acc, value) => (acc._1 + Math.abs(value._1), acc._2 + value._3),
       (acc, value) => (acc._1 + value._1, acc._2 + value._2)
     )
    val MAE = MAE_values._1 / MAE_values._2
    System.out.println(s"MAE is $MAE")

    //calculate MAPE, based on https://en.wikipedia.org/wiki/Mean_absolute_percentage_error
    val MAPE_Values = estimatesAndObserved.aggregate((0.0, 0.0)) (
      (acc, value) =>  {
        val difference = value._4
        val absoluteValue = Math.abs(difference / value._2)
        (acc._1 + absoluteValue, acc._2 + value._3)
      },
      (acc,value) => (acc._1 + value._1, acc._2+value._2)
    )
    val MAPE = (100/MAPE_Values._2) * MAPE_Values._1
    System.out.println(s"MAPE is $MAPE")
  }


  def main(args:Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("modelEvaluation")
    val sc = new SparkContext(conf)
    //first argument will be the path for the data
    val modelPath = args(0)
    val testDataSetPath = args(1)
    processFiles(modelPath, testDataSetPath, sc)
  }
}
