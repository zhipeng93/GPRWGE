package apps

import org.apache.spark.{SparkContext, SparkConf}
import utils.ArgsUtil

object Clean{
	def cleanOneLine(line: String): String = {
		var cleaned = line.filter(c => c.isLetter || c == ' ').replaceAll("\\s{1,}", " ")
		if(cleaned.startsWith(" "))
			cleaned = cleaned.substring(1)
		cleaned
	}
	def main(args: Array[String]): Unit = {
		val params = ArgsUtil.parse(args)
		val input = params.getOrElse("INPUT", "testin")
		val output = params.getOrElse("OUTPUT", "testout")
		val numPartitions = params.getOrElse("PARTS", "10").toInt
		val sc = new SparkContext(new SparkConf().setAppName("repartition data"))
		val inData = sc.textFile(input).repartition(numPartitions)
		inData.map(line => {
			cleanOneLine(line)
		}).saveAsTextFile(output)

	}
}