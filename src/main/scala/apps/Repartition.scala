package apps

import org.apache.spark.{SparkContext, SparkConf}
import utils.ArgsUtil

object Repartition{
	def main(args: Array[String]): Unit = {
		val params = ArgsUtil.parse(args)
		val input = params.getOrElse("INPUT", "testin")
		val output = params.getOrElse("OUTPUT", "testout")
		val numPartitions = params.getOrElse("PARTS", "10").toInt
		val sc = new SparkContext(new SparkConf().setAppName("repartition data"))
		sc.textFile(input).repartition(numPartitions).saveAsTextFile(output)
	}
}