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
import utils.{HashMapModel, PairsDataset, _}

import scala.collection.mutable


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

		/* re-organize and shuffle the training data according to the source Meta */
		/* cannot use the block-SGD because we do not have the whole graph. */
		/* Now it is wrong since different workers may conflict on the dst vectors. */
		/* one element in each partition. */
		val dataRDDForJoin: RDD[(Int, PairsDataset)] = batchData.mapPartitionsWithIndex( (thisPid, iter) => {
			// one batch contains only one PairsDataset
			val pairsDataset = iter.next()
			val dSrc = pairsDataset.src
			val dDst = pairsDataset.dst
			logInfo(s"*ghand*number of data in each partition: ${dSrc.size}")

			val srcMeta: Array[Int] = bcMeta.value.srcMeta
			// for partition the training data
			val dataSrc: Array[ArrayBuffer[Int]] = Array.ofDim(numPartititions)
			val dataDst: Array[ArrayBuffer[Int]] = Array.ofDim(numPartititions)
			// store the index of dstModel that this workers needs.
			val indexDst: Array[ArrayBuffer[Int]] = Array.ofDim(numPartititions)
			for(i <- 0 until(dataSrc.length)){
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
		dataRDDForJoin.cache()

		/* compute the index of each dstModel needed by each partition */
		val dstKeys: RDD[(Int, Iterable[PullKey])] = dataRDDForJoin.mapPartitionsWithIndex((pid, iter) => {
			val (_, pairsDataset) = iter.next()
			val dstMeta = bcMeta.value.dstMeta
			val dSrc = pairsDataset.src
			val dDst = pairsDataset.dst
			val indexDstToPull: Array[ArrayBuffer[Int]] = Array.ofDim(numPartititions)
			for(i <- 0 until(indexDstToPull.length)){
				indexDstToPull(i) = new ArrayBuffer[Int]()
			}
			for(i <- 0 until(dDst.length)){
				val dstId = dDst(i)
				if(dstMeta(dstId) != pid){
					// this is where our partitioner works
					indexDstToPull(dstMeta(dstId)) += dstId
				}
			}
			indexDstToPull.map(x => new PullKey(pid, x)).zipWithIndex.map(x => (x._2, x._1)).toIterator
		}).groupByKey(numPartititions)

		/* get the dstModels from each server Id, organize them according to the partition id*/
		val dstModelForJoin: RDD[(Int, HashMapModel)] = dstKeys.zipPartitions(modelRDD)((iter1, iter2) => {
			val (pid, keys): (Int, Iterable[PullKey]) = iter1.next()
			val (mid, (srcModel, dstModel)): (Int, (HashMapModel, HashMapModel)) = iter2.next()
			val modelsToSend: Array[HashMapModel] = Array.ofDim(numPartititions)
			for(i <- 0 until(modelsToSend.length)){
				modelsToSend(i) = new HashMapModel
			}
			keys.foreach(pkey => {
				val workerId = pkey.workerId
				val dstKeys = pkey.keys
				for(i <- 0 until(dstKeys.length)){
					val dstId = dstKeys(i)
					modelsToSend(workerId).put(dstId, dstModel.get(dstId))
				}
			})
			modelsToSend.zipWithIndex.map(x => (x._2, x._1)).toIterator
		}).reduceByKey((x, y) => x.merge(y))

		/* train the model */
		val tmp: RDD[(Float, Array[Array[Float]], Int, HashMapModel)] = modelRDD
			.zipPartitions(dataRDDForJoin, dstModelForJoin)((iter1, iter2, iter3) => {
			val (mid, (srcModel, dstModel)) = iter1.next()
			val (did, pairsTrainset) = iter2.next()
			val (dstMid, netDstModel) = iter3.next()
			assert(mid == did && dstMid == mid)
			val negativeSamplesModel: Array[Array[Float]] = bcNegDstModel.value
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
		})

//		val dataPlusSrcModel: RDD[(Int, (PairsDataset, (HashMapModel, HashMapModel)))] =
//			dataRDDForJoin.zipPartitions(modelRDD)(
//				(iter1, iter2) => {
//					// one element in each partition of dataRDDForJoin
//					// one element in each partition of modelRDD
//					val data = iter1.next()
//					val model = iter2.next()
//					assert(data._1 == model._1) // zip should follow this.
//					Iterator.single((data._1, (data._2, model._2)))
//				}
//			)
//
//		// shuffle the dst embeddings, according to the meta data
//		// if the dstModelVec is already in the partition, then no shuffle is needed.
//		// this is where our partitioner for reducing communication works.
//
//		val dstModelForJoin: RDD[(Int, HashMapModel)] = dataPlusSrcModel.mapPartitions(x => {
//			// one elements in each partition.
//			val (pid, (pairsDataset, (srcModel, dstModel))) = x.next()
//
//			val dDst = pairsDataset.dst
//			val dstMeta: Array[Int] = bcMeta.value.dstMeta
//			val arrayDstModel: Array[HashMapModel] = Array.ofDim(numPartititions)
//			// init
//			for (i <- 0 until (arrayDstModel.length)) {
//				arrayDstModel(i) = new HashMapModel()
//			}
//			for (i <- 0 until (dDst.length)) {
//				val dstPid = dstMeta(dDst(i))
//				if (dstPid != pid) {
//					// the dst embeddings are not in this partition, should be shuffled.
//					arrayDstModel(pid).put(dDst(i), dstModel.get(dDst(i)))
//				}
//			}
//			arrayDstModel.zipWithIndex.map(x => (x._2, x._1)).toIterator
//		}).reduceByKey((x, y) => x.merge(y))

		// train the model
//		val tmp: RDD[(Float, Array[Array[Float]], Int, HashMapModel)] = dataPlusSrcModel
//			.zipPartitions(dstModelForJoin)((iter1, iter2) =>{
//			val (pid, (pairsTrainset, (srcModel, dstModel))) = iter1.next()
//			val (pid2, netDstModel) = iter2.next()
//			assert(pid == pid2)
//			// the update on the negative samples happens on negative samples.
//			// we need to collect them.
//			val negativeSamplesModel: Array[Array[Float]] = bcNegDstModel.value
//			val negUpdate: Array[Array[Float]] = Array.ofDim(negativeSamplesModel.length, dimension)
//
//			val srcIds = pairsTrainset.src
//			val dstIds = pairsTrainset.dst
//			var loss = 0.0f
//			for (i <- 0 until (srcIds.length)) {
//				val srcVec = srcModel.get(srcIds(i))
//				var dstVec: Array[Float] = null
//
//				if (dstModel.containsKey(dstIds(i))) {
//					dstVec = dstModel.get(dstIds(i))
//				}
//				else {
//					// it mush be transferred here.
//					assert(netDstModel.containsKey(dstIds(i)))
//					dstVec = netDstModel.get(dstIds(i))
//				}
//				loss += trainOnePair(srcVec, dstVec, negativeSamplesModel, negUpdate)
//			}
//			Iterator.single((loss, negUpdate, srcIds.length, netDstModel))
//		})

		// deal with the loss
		val (batchLoss, negUpdate, batchCnt): (Float, Array[Array[Float]], Int) = tmp.mapPartitions(iter => {
			// one element per partition
			val ele = iter.next()
			Iterator.single((ele._1, ele._2, ele._3))
		}).reduce((x, y) => (x._1 + y._1, FastMath.inPlaceVecAdd(x._2, y._2), x._3 + y._3))

		// recover the negative updates, as well as the updates to the vectors
		FastMath.vecDotScalar(negUpdate, 1.0f / batchCnt)
		val bcNegUpdate: Broadcast[Array[Array[Float]]] = modelRDD.sparkContext.broadcast(negUpdate)

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
		}).reduceByKey((x, y) => x.merge(y))
		// update them
		modelRDD.zipPartitions(updatedDstForJoin)((iter1, iter2) => {
			val (pid, (srcModel, dstModel)) = iter1.next()
			val (pid2, updatedModel) = iter2.next()
			assert(pid == pid2)
			val entryIter = updatedModel.toEntryterator()
			while(entryIter.hasNext){
				val entry = entryIter.next()
				val key = entry.getKey
				val value = entry.getValue
				// should add careful locking mechanism, as a PS control.
				dstModel.update(key, value)
			}

			// update the negative update value
			val localNegUpdate: Array[Array[Float]] = bcNegUpdate.value
			val localIds: Array[Int] = bcNegArray.value
			for(i <- 0 until(localIds.length)){
				val key = localIds(i)
				val value = localNegUpdate(i)
				if(dstModel.containsKey(key)){
					dstModel.updateAdd(key, value)
				}
			}
			Iterator.single(1)
		}).count()

		dataRDDForJoin.unpersist()

		logInfo(s"*ghand*batch finished, batchLoss: ${batchLoss / batchCnt}, batchCnt:${batchCnt}")
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
		var start_time = System.currentTimeMillis()
		val sparkContext = new SparkContext(new SparkConf().setAppName("GraphEmebdding"))
		val (chunkedDataset, bcDict, vertexNum): (RDD[(ChunkDataset, Int)],
			Broadcast[Array[String]], Int) = loadData(sparkContext)
		chunkedDataset.setName("chunkedDatasetRDD").cache()
		val numChunks: Int = chunkedDataset.count().toInt
		logInfo(s"*ghand*finished loading data, n" +
			s"um of chunks:${numChunks}, num of vertex:${vertexNum}, " +
			s"time cost: ${(System.currentTimeMillis() - start_time) / 1000.0f}")
		start_time = System.currentTimeMillis()
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
		modelRDD.setName("modelRDD").cache()

		val batchIter = sampler.batchIterator()
		logInfo(s"*ghand* num of data points in one example chunk: ${chunkedDataset.take(1)(0)._1.numElements}")


		val databatch: RDD[PairsDataset] = batchIter.next()
		val numEleBatch = databatch.mapPartitions(iter => {
			val ele = iter.next()
			Iterator.single(ele.toArrays()._2.length)
		}).reduce((x, y) => x + y)
		logInfo(s"*ghand* num of ele in one batch:${numEleBatch}")


		for (epochId <- 0 until (numEpoch)) {
			var trainedLoss = 0.0
			var trainedPairs = 0
			var batchId = 0
			while (batchId < numChunks) {
				val batchData: RDD[PairsDataset] = batchIter.next()
				val (batchLoss, batchCnt) = train(batchData, bcMeta, modelRDD, vertexNum, batchId)
				trainedLoss += batchLoss
				trainedPairs += batchCnt
				batchId += 1
			}
			logInfo(s"*ghand*epochId:${epochId} trainedPairs:${trainedPairs} loss:${trainedLoss / trainedPairs}")

			if (((epochId + 1) % checkpointInterval) == 0) {
				// checkpoint the model
				saveModel(modelRDD.map(x => x._2._1), bcDict, output + "_src_" + epochId)
				saveModel(modelRDD.map(x => x._2._2), bcDict, output + "_dst_" + epochId)
			}
		}
	}

}
