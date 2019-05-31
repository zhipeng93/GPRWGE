package utils

import org.apache.spark.SparkConf
object SparkUtils {

	def getNumExecutors(conf: SparkConf): Int = {
		if (conf.getBoolean("spark.dynamicAllocation.enabled", false))
			conf.getInt("spark.dynamicAllocation.maxExecutors", 10)
		else
			conf.getInt("spark.executor.instances", 10)
	}

	def getNumCores(conf: SparkConf): Int = {
		val numExecutors = getNumExecutors(conf)
		val coresForExecutor = conf.getInt("spark.executor.cores", 1)
		numExecutors * coresForExecutor
	}
}