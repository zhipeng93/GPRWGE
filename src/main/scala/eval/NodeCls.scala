package eval

import utils.ArgsUtil
import ge.EmbeddingConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionSummary
import org.apache.spark.sql.{SparkSession}

object NodeCls{
	def main(args: Array[String]): Unit = {
		val params = ArgsUtil.parse(args)
		val embFile = params.getOrElse(EmbeddingConf.FEATURES, "input.features")
		val labelFile = params.getOrElse(EmbeddingConf.LABELS, "input.labels")

		val builder = SparkSession.builder().appName("Node Classification")
		val spark = builder.getOrCreate()
		val sparkContext = spark.sparkContext

		val DELIMITER = "[\\s+|,]"
		val features: RDD[(String, Array[Double])] = sparkContext.textFile(embFile)
			.filter(x => x != null && x.length > 0 && !x.startsWith("#"))
			.map(x => x.stripLineEnd.split(DELIMITER)).map(x => (x(0), x.slice(1, x.length).map(i => i.toDouble)))
		val labels: RDD[(String, Int)] = sparkContext.textFile(labelFile)
			.filter(x => x != null && x.length > 0 && !x.startsWith("#"))
			.map(x => x.stripLineEnd.split(DELIMITER)).map(x => (x(0), x(1).toInt))
		val dataRDD = labels.join(features)
			.map(x => {
				val label = x._2._1
				val features = x._2._2
				(label, features)
			})
		val dataDF = spark.createDataFrame(dataRDD).toDF("label", "features")
		val splittedData = dataDF.randomSplit(Array(0.8, 0.2), 2019)
		val trainset = splittedData(0)
		val testset = splittedData(1)

		val model = new LogisticRegression()
    		.setMaxIter(100)
		val fitted = model.fit(trainset)
		val accs: LogisticRegressionSummary = fitted.evaluate(testset)
		println(s"accuracy: ${accs.accuracy}, " +
			s"truePostive: ${accs.truePositiveRateByLabel.mkString("-")}, " +
			s"${accs.falsePositiveRateByLabel.mkString("-")}, " +
			s"${accs.fMeasureByLabel.mkString("-")}, " +
			s"${accs.precisionByLabel.mkString("-")}, " +
			s"${accs.recallByLabel.mkString("-")}")


	}
}