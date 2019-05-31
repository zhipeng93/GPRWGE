package ge

import java.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.internal.Logging
import models._
import org.apache.spark.broadcast.Broadcast
import utils._


class GraphEmbedding(val params: Map[String, String]) extends Serializable with Logging {

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

	/**
	  * load the data into chunks, each chunk with size=batchSize
	  *
	  * @return chunkedDataset and mappings
	  */
	def loadData(sparkContext: SparkContext): (RDD[(ChunkDataset, Int)], Broadcast[Array[String]], Int) = {
		val numCores = SparkUtils.getNumCores(sparkContext.getConf)
		val textFile = sparkContext.textFile(input, minPartitions = numCores * 2)
		Features.corpusStringToChunkedIntWithRemapping(textFile, batchSize)
	}

	/**
	  * each model partition contains a model partition as well
	  * as one partitionId, for join with Datasets
	  *
	  * @param sampler
	  * @param bcMeta
	  * @return (pid + wordModel + ctxModel)
	  */
	def initModel(sampler: BaseModel, bcMeta: Broadcast[DistributionMeta]): (RDD[(Int, (HashMapModel, HashMapModel))]) = {

		val trainSet = sampler.trainset
		val model: RDD[(Int, (HashMapModel, HashMapModel))] =
			trainSet.mapPartitionsWithIndex((pid, iter) => {
				val srcMeta: Array[Int] = bcMeta.value.srcMeta
				val dstMeta: Array[Int] = bcMeta.value.dstMeta

				val srcModel: HashMapModel = new HashMapModel()
				val dstModel: HashMapModel = new HashMapModel()
				val random = new Random(pid)
				for (i <- 0 until (srcMeta.length)) {
					if (srcMeta(i) == pid) {
						srcModel.put(i, initOneDimModel(dimension, 0.5.toFloat / dimension, random))
					}
					if (dstMeta(i) == pid) {
						dstModel.put(i, initOneDimModel(dimension, 0.5.toFloat / dimension, random))
					}
				}
				Iterator.single((pid, (srcModel, dstModel)))
			})
		model.cache()
		model
	}

	def initOneDimModel(size: Int, bandwidth: Float, random: Random): Array[Float] = {
		val array = Array.ofDim[Float](size)
		for (i <- 0 until (array.length)) {
			array(i) = (random.nextFloat() - 0.5.toFloat) * 2 * bandwidth
		}
		array
	}

	def train(batchData: RDD[PairsDataset], bcMeta: Broadcast[DistributionMeta],
			  modelRDD: RDD[(Int, (HashMapModel, HashMapModel))], vertexNum: Int,
			  iterationId: Int): (Float, Int) = {
		val numPartititions = batchData.getNumPartitions

		/* re-organize and shuffle the training data according to the source Meta */
		// should use the block-SGD.
		// Now it is wrong since different workers may conflict on the dst vectors.
		// multiple elements in each partition.
		val dataRDDForJoin: RDD[(Int, PairsDataset)] = batchData.mapPartitions(iter => {
			// one batch contains only one PairsDataset
			val data: PairsDataset = iter.next()
			val dSrc = data.src
			val dDst = data.dst
			val srcMeta: Array[Int] = bcMeta.value.srcMeta
			val arraySrc: Array[ArrayBuffer[Int]] = Array.ofDim(numPartititions)
			val arrayDst: Array[ArrayBuffer[Int]] = Array.ofDim(numPartititions)
			for(i <- 0 until(arraySrc.length)){
				arraySrc(i) = new ArrayBuffer[Int]()
				arrayDst(i) = new ArrayBuffer[Int]()
			}
			for (i <- 0 until (dSrc.length)) {
				val pid = srcMeta(dSrc(i))
				arraySrc(pid) += dSrc(i)
				arrayDst(pid) += dDst(i)
			}
			arrayDst(0).size
			val zipped: Array[((ArrayBuffer[Int], ArrayBuffer[Int]), Int)] = arraySrc.zip(arrayDst).zipWithIndex
			zipped.map(x => (x._2, PairsDataset(x._1._1.toArray, x._1._2.toArray))).toIterator
		})

		/* shared negative sampling */
		// first generate the negativeIDs on the driver,
		// then broadcast these IDs to all workers and collect their dstEmbeddings to driver
		// broadcast dstEmbeddings to all workers
		val random: Random = new Random()
		val negArray: Array[Int] = Array.ofDim(negative)
		for (i <- 0 until (negArray.length)) {
			negArray(i) = random.nextInt(vertexNum)
		}
		val bcNegArray = modelRDD.sparkContext.broadcast(negArray)
		val negDstModel: Array[(Int, Array[Float])] = modelRDD.mapPartitions(iter => {
			// one element per partition
			val (pid, (srcModel, dstModel)) = iter.next()
			val localNegArray = bcNegArray.value
			val arrayBuffer: ArrayBuffer[(Int, Array[Float])] = new ArrayBuffer[(Int, Array[Float])]()
			for (i <- 0 until (localNegArray.length)) {
				val dstId = localNegArray(i)
				if(dstModel.containsKey(dstId)){
					arrayBuffer.append((dstId, dstModel.get(dstId)))
				}
			}
			arrayBuffer.toIterator
		}).collect()
		// we do not broadcast ids of negative samples, since they are the same logically.
		val bcNegDstModel: Broadcast[Array[Array[Float]]] =
			modelRDD.sparkContext.broadcast(negDstModel.map(x => x._2))

		val dataPlusSrcModel: RDD[(Int, (PairsDataset, (HashMapModel, HashMapModel)))] =
			dataRDDForJoin.join(modelRDD)


		// shuffle the dst embeddings, according to the meta data
		// if the dstModelVec is already in the partition, then no shuffle is needed.

		val dstModelForJoin: RDD[(Int, HashMapModel)] = dataPlusSrcModel.map(x => {
			// multiple elements in each partition.
			val (pid, (pairsDataset, (srcModel, dstModel))) = x

			val dDst = pairsDataset.dst
			val dstMeta: Array[Int] = bcMeta.value.dstMeta
			val arrayDstModel: Array[HashMapModel] = Array.ofDim(numPartititions)
			// init
			for (i <- 0 until (arrayDstModel.length)) {
				arrayDstModel(i) = new HashMapModel()
			}
			for (i <- 0 until (dDst.length)) {
				val dstPid = dstMeta(dDst(i))
				if (dstPid != pid) {
					// the dst embeddings are not in this partition, should be shuffled.
					arrayDstModel(dstPid).put(dDst(i), dstModel.get(dDst(i)))
				}
			}
			arrayDstModel.zipWithIndex.map(x => (x._2, x._1)).toIterator
		}).flatMap(x => x)

		// train the model
		// val (batchLoss, dstUpdate, batchCnt): (Float, Int, RDD[(Int, HashMapModel)]) = dataPlusSrcModel
		val tmp: RDD[(Float, Array[Array[Float]], Int, HashMapModel)] = dataPlusSrcModel
			.join(dstModelForJoin).map(x => {
			val (pid, ((pairsTrainset, (srcModel, dstModel)), netDstModel)) = x
			// the update on the negative samples happens on negative samples.
			// we need to collect them.
			val negativeSamplesModel: Array[Array[Float]] = bcNegDstModel.value
			logInfo(s"number of negative samples used:${negativeSamplesModel.length}")
			val negUpdate: Array[Array[Float]] = Array.ofDim(negativeSamplesModel.length, dimension)

			val srcIds = pairsTrainset.src
			val dstIds = pairsTrainset.dst
			var loss = 0.0f
			for (i <- 0 until (srcIds.length)) {
				val srcVec = srcModel.get(srcIds(i))
				var dstVec: Array[Float] = null

				if (dstModel.containsKey(dstIds(i))) {
					dstVec = dstModel.get(dstIds(i))
				}
				else {
					// it mush be transferred here.
					assert(netDstModel.containsKey(dstIds(i)))
					dstVec = netDstModel.get(dstIds(i))
				}
				loss += trainOnePair(srcVec, dstVec, negativeSamplesModel, negUpdate)
			}
			Iterator.single((loss, negUpdate, srcIds.length, netDstModel))
		}).flatMap(x => x)

		// deal with the loss
		val (batchLoss, negUpdate, batchCnt): (Float, Array[Array[Float]], Int) = tmp.mapPartitions(iter => {
			// one element per partition
			val ele = iter.next()
			Iterator.single((ele._1, ele._2, ele._3))
		}).reduce((x, y) => (x._1 + y._1, FastMath.inPlaceVecAdd(x._2, y._2), x._3 + y._3))

		// recover the negative updates, as well as the updates to the vectors


		// shuffle the update of contexts, make it happen.


		val updatedDstForJoin: RDD[(Int, HashMapModel)] = tmp.mapPartitions(iter => {
			val hashMapModel: HashMapModel = iter.next()._4
			val dstMeta = bcMeta.value.dstMeta
			val arrayHashMapModel: Array[HashMapModel] = Array.ofDim(numPartititions)
			for (i <- 0 until (arrayHashMapModel.length))
				arrayHashMapModel(i) = new HashMapModel
			val entryIter = hashMapModel.toEntryterator()
			while (entryIter.hasNext) {
				val entry = entryIter.next()
				val dstId = entry.getKey
				arrayHashMapModel(dstMeta(dstId)).put(dstId, hashMapModel.get(dstId))
			}
			arrayHashMapModel.zipWithIndex.toIterator.map(x => (x._2, x._1))
		})



//		val updatedDstForJoin: RDD[(Int, HashMapModel)] = tmp.mapPartitions(iter => {
//			val hashMapModel: HashMapModel = iter.next()._4
//			val dstMeta = bcMeta.value.dstMeta
//			val arrayHashMapModel: Array[HashMapModel] = Array.ofDim(numPartititions)
//			for (i <- 0 until (arrayHashMapModel.length))
//				arrayHashMapModel(i) = new HashMapModel
//			val entryIter = hashMapModel.toEntryterator()
//			while (entryIter.hasNext) {
//				val entry = entryIter.next()
//				val dstId = entry.getKey
//				arrayHashMapModel(dstMeta(dstId)).put(dstId, hashMapModel.get(dstId))
//			}
//			arrayHashMapModel.zipWithIndex.toIterator.map(x => (x._2, x._1))
//		})

		// update the wordEmbeddings
		modelRDD.join(updatedDstForJoin).mapPartitions(iter => {
			// one element per partition
			val (pid, ((srcModel, dstModel), updateDstModel)) = iter.next()
			val entryIter = updateDstModel.toEntryterator()
			while (entryIter.hasNext) {
				val entry = entryIter.next()
				val dstId = entry.getKey
				val delta = entry.getValue
				dstModel.updateAdd(dstId, delta)
			}
			Iterator.single(1)
		}).count // make the update to the context happen


		// normalize the negUpdate, and shuffle the negative update: negUpdate
		FastMath.vecDotScalar(negUpdate, 1.0f / batchCnt)
		val negHashmapModel = new HashMapModel
		for (i <- 0 until (negUpdate.length)) {
			negHashmapModel.put(negDstModel(i)._1, negUpdate(i))
		}
		val bcNegHashmapModel = modelRDD.sparkContext.broadcast(negHashmapModel)
		modelRDD.mapPartitions(iter => {
			val (pid, (srcModel, dstModel)) = iter.next()
			val localBcNegHashmapModel = bcNegHashmapModel.value
			val entryIter = localBcNegHashmapModel.toEntryterator()
			while (entryIter.hasNext) {
				val entry = entryIter.next()
				val key = entry.getKey
				val value = entry.getValue
				if(dstModel.containsKey(key)) {
					dstModel.updateAdd(key, value)
				}
			}
			Iterator.single(1)
		}).count()

		logInfo(s"batch finished, batchLoss: ${batchLoss / batchCnt}")
		(batchLoss, batchCnt)
	}

	def trainOnePair(srcVec: Array[Float], dstVec: Array[Float], sharedNegativeModels: Array[Array[Float]],
					 sharedNegativeUpdate: Array[Array[Float]]): Float = {
		var loss: Float = 0.0f
		val srcUpdate: Array[Float] = Array.ofDim(dimension)

		// positive pair
		var label = 1
		val prob = FastMath.sigmoid(FastMath.vecDotVec(srcVec, dstVec))
		val g = -(label - prob) * stepSize

		for (i <- 0 until (srcVec.length)) {
			srcUpdate(i) -= g * dstVec(i)
			dstVec(i) -= g * srcVec(i)
		}
		loss += -FastMath.log(prob)

		// negative pairs
		for (negId <- 0 until (negative)) {
			label = 0
			val prob = FastMath.sigmoid(FastMath.vecDotVec(srcVec, sharedNegativeModels(negId)))
			val g = -(label - prob) * stepSize

			for (i <- 0 until (srcVec.length)) {
				srcUpdate(i) -= g * sharedNegativeModels(negId)(i)
				sharedNegativeUpdate(negId)(i) -= g * srcVec(i)
			}
			loss += -FastMath.log(1 - prob)
		}

		for (i <- 0 until (srcVec.length)) {
			srcVec(i) += srcUpdate(i)
		}
		loss
	}


	def saveModel(model: RDD[HashMapModel],
				  bcDict: Broadcast[Array[String]], output: String): Unit = {

		// adapted from hadoop saveAsText, only save the source vector
		val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
		val textClassTag = implicitly[ClassTag[Text]]


		val arrayModel = model.mapPartitions(x => {
			// one element in each partition
			val int2String = bcDict.value
			val vertexEntryIter = x.next().toEntryterator()
			val arrayBuffer = new ArrayBuffer[(String, Array[Float])](1000)
			while (vertexEntryIter.hasNext()) {
				val entry = vertexEntryIter.next()
				arrayBuffer.append((int2String(entry.getKey), entry.getValue))
			}
			arrayBuffer.toIterator
		})

		val r = arrayModel.mapPartitions(iter => {
			val text = new Text()
			iter.map(x => {
				text.set(x._1 + " " + x._2.mkString(" "))
				(NullWritable.get(), text)
			})
		})
		RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
			.saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](output)
	}

	def run(): Unit = {
		val sparkContext = new SparkContext(new SparkConf().setAppName("GraphEmebdding"))
		val (chunkedDataset, bcDict, vertexNum): (RDD[(ChunkDataset, Int)],
			Broadcast[Array[String]], Int) = loadData(sparkContext)

		val sampler: BaseModel = samplerName match {
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

		val modelRDD = initModel(sampler, bcMeta)

		for (epochId <- 0 until (numEpoch)) {
			val batchIter = sampler.batchIterator()
			var trainedLoss = 0.0
			var trainedPairs = 0
			var batchId = 0
			while (batchIter.hasNext) {
				val batchData: RDD[PairsDataset] = batchIter.next()
				val (batchLoss, batchCnt) = train(batchData, bcMeta, modelRDD, vertexNum, batchId)
				trainedLoss += batchLoss
				trainedPairs += batchCnt
				batchId += 1
			}
			logInfo(s"epochId:${epochId} trainedPairs:${trainedPairs} loss:${trainedLoss / trainedPairs}")

			if (((epochId + 1) % checkpointInterval) == 0) {
				// checkpoint the model
				saveModel(modelRDD.map(x => x._2._1), bcDict, output + "_src_" + epochId)
				saveModel(modelRDD.map(x => x._2._2), bcDict, output + "_dst_" + epochId)
			}
		}
	}

}
