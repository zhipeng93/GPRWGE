package apps

import org.apache.spark.{SparkContext, SparkConf}
import utils.ArgsUtil

object Sample{
	def main(args: Array[String]): Unit = {
		val params = ArgsUtil.parse(args)
		val input = params.getOrElse("INPUT", "testin")
		val output = params.getOrElse("OUTPUT", "testout")
		val ratio: Double = params.getOrElse("RATIO", "0.1").toDouble
		val sc = new SparkContext(new SparkConf().setAppName("subsample data"))
		sc.textFile(input).sample(withReplacement = false, ratio).saveAsTextFile(output)
	}
}