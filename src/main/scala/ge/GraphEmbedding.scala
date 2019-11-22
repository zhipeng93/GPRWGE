package ge

import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.internal.Logging
import samplers._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import ge.basics.{PairsDataset, ChunkDataset, DistributionMeta}
import scala.collection.mutable.ArrayBuffer
import utils.{SparkUtils, Features}

abstract class GraphEmbedding(val params: Map[String, String]) extends Serializable with Logging {

	val dimension = params.getOrElse(EmbeddingConf.EMBEDDINGDIM, "100").toInt
	val negative = params.getOrElse(EmbeddingConf.NEGATIVESAMPLENUM, "5").toInt
	val window = params.getOrElse(EmbeddingConf.WINDOWSIZE, "5").toInt
	val stepSize = params.getOrElse(EmbeddingConf.STEPSIZE, "0.1").toFloat
	val stopRate = params.getOrElse(EmbeddingConf.STOPRATE, "0.15").toFloat
	val numEpoch = params.getOrElse(EmbeddingConf.NUMEPOCH, "10").toInt
	val checkpointInterval = params.getOrElse(EmbeddingConf.CHECKPOINTINTERVAL, numEpoch.toString).toInt
	val samplerName = params.getOrElse(EmbeddingConf.SAMPLERNAME, "line").toUpperCase()
	val input = params.getOrElse(EmbeddingConf.INPUTPATH, "input")
	val output = params.getOrElse(EmbeddingConf.OUTPUTPATH, "output")
	val batchSize = params.getOrElse(EmbeddingConf.BATCHSIZE, "batchsize").toInt
	val platForm = params.getOrElse(EmbeddingConf.PLATFORM, "SPARK")

	/**
	  * load the data into chunks, each chunk with size=batchSize
	  *
	  * @return chunkedDataset and mappings
	  */
	def loadData(sparkContext: SparkContext): (RDD[(ChunkDataset, Int)], Broadcast[Array[String]], Int) = {
		val numCores = SparkUtils.getNumCores(sparkContext.getConf)
		// coalesce does not support barrier mode.
		// The reason given by mengxr is "when shuffle is false,
		// it may lead to different number of partitions"
		val textFile = sparkContext.textFile(input, minPartitions = numCores).repartition(numCores)
		Features.corpusStringToChunkedIntWithRemapping(textFile, batchSize)
	}

	/**
	  *
	  * @param batchData it is already shuffled by source.
	  * @param bcMeta
	  * @param gEModel
	  * @param vertexNum
	  * @param iterationId
	  * @return
	  */
	def train(batchData: RDD[(Int, PairsDataset)], bcMeta: Broadcast[DistributionMeta],
			  gEModel: GEModel, vertexNum: Int, iterationId: Int): (Float, Int) = ???

	def initModel(sampler: BaseSampler, bcMeta: Broadcast[DistributionMeta]): GEModel = ???

	def shuffleDataBySource(batchData: RDD[PairsDataset], bcMeta: Broadcast[DistributionMeta]): RDD[(Int, PairsDataset)] = {

		val numPartititions = batchData.getNumPartitions
		// use barrier mode for rdd data generating.
		batchData.mapPartitions(iter => {
			// one batch contains only one PairsDataset
			val pairsDataset = iter.next()
			val dSrc = pairsDataset.src
			val dDst = pairsDataset.dst

			val srcMeta: Array[Int] = bcMeta.value.srcMeta
			// for partition the training data
			val dataSrc: Array[ArrayBuffer[Int]] = Array.ofDim(numPartititions)
			val dataDst: Array[ArrayBuffer[Int]] = Array.ofDim(numPartititions)
			for (i <- 0 until (dataSrc.length)) {
				dataSrc(i) = new ArrayBuffer[Int]()
				dataDst(i) = new ArrayBuffer[Int]()
			}
			for (i <- 0 until (dSrc.length)) {
				// send this data point to pid
				val srcPid = srcMeta(dSrc(i))
				dataSrc(srcPid) += dSrc(i)
				dataDst(srcPid) += dDst(i)
			}

			val zippedData: Array[(Int, PairsDataset)] = dataSrc.zip(dataDst)
				.zipWithIndex.map(x => (x._2, new PairsDataset(x._1._1, x._1._2)))
			zippedData.toIterator
		}).reduceByKey((x, y) => x.merge(y))
	}


	def run(): Unit = {

		val spark: SparkSession= SparkSession.builder().appName("GraphEmbedding-" + platForm).getOrCreate()
		val sparkContext = spark.sparkContext

		/* loading data */
		var start_time = System.currentTimeMillis()
		val (chunkedDataset, bcDict, vertexNum): (RDD[(ChunkDataset, Int)],
			Broadcast[Array[String]], Int) = loadData(sparkContext)
		chunkedDataset.setName("chunkedDatasetRDD").cache()
		val numChunks: Int = chunkedDataset.count().toInt
		logInfo(s"*ghand*finished loading data, n" +
			s"um of chunks:${numChunks}, num of vertex:${vertexNum}, " +
			s"time cost: ${(System.currentTimeMillis() - start_time) / 1000.0f}")

		start_time = System.currentTimeMillis()
		val sampler: BaseSampler = samplerName match {
			case "APP" =>
				new APP(chunkedDataset, vertexNum, stopRate)
			case "DEEPWALK" =>
				new DeepWalk(chunkedDataset, vertexNum, window)
			case "LINE" =>
				new LINE(chunkedDataset, vertexNum)
		}

		/**
		  * for now we don't split vertex. Later will modify this for power-law.
		  */
		val meta: DistributionMeta = sampler.hashDestinationForNodes(sampler.trainset.getNumPartitions)
		val bcMeta = sparkContext.broadcast(meta)
		val geModel: GEModel = initModel(sampler, bcMeta)

		val batchIter = sampler.batchIterator() // barrierRDD.
		val trainStart = System.currentTimeMillis()
		for (epochId <- 0 until (numEpoch)) {
			var trainedLoss = 0.0
			var trainedPairs = 0
			var batchId = 0
			while (batchId < numChunks) {
				val batchData: RDD[PairsDataset] = batchIter.next() // barrierRDD
				val shuffledData = shuffleDataBySource(batchData, bcMeta)
//				shuffledData.cache()
				val (batchLoss, batchCnt) = train(shuffledData, bcMeta, geModel, vertexNum, batchId)
//				shuffledData.unpersist()
				trainedLoss += batchLoss
				trainedPairs += batchCnt
				batchId += 1
				logInfo(s"*ghand*epochId:${epochId} batchId:${batchId} " +
					s"batchPairs:${batchCnt} loss:${batchLoss / batchCnt}")

			}
			logInfo(s"*ghand*epochId:${epochId} trainedPairs:${trainedPairs} " +
				s"loss:${trainedLoss / trainedPairs}")

			if (((epochId + 1) % checkpointInterval) == 0) {
				// checkpoint the model
				geModel.saveSrcModel(output + "_src_" + epochId, bcDict)
				geModel.saveDstModel(output + "_dst_" + epochId, bcDict)
			}
		}
		logInfo(s"*ghand* training ${numEpoch} epochs takes: " +
			s"${(System.currentTimeMillis() - trainStart) / 1000.0} seconds")
		geModel.destory()
	}

}
