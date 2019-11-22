package ge

import java.util.Random

import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import ge.basics.{HashMapModel, PairsDataset, DistributionMeta}
import ge.PSUtils._
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import org.apache.spark.TaskContext
import samplers.BaseSampler
import utils.FastMath


class GEPS(params: Map[String, String]) extends GraphEmbedding(params: Map[String, String]) {

	val numParts = params.getOrElse(EmbeddingConf.NUMPARTS, "10").toInt
	val numNodePerRow = params.getOrElse(EmbeddingConf.NUMNODEPERROW, "10000").toInt

	override def initModel(sampler: BaseSampler, bcMeta: Broadcast[DistributionMeta]): GEModel = {
		new GEPSModel(sampler, bcMeta, dimension, numNodePerRow, numParts)
	}

	override def train(batchData: RDD[(Int, PairsDataset)], bcMeta: Broadcast[DistributionMeta],
					   gEModel: GEModel, vertexNum: Int, iterationId: Int): (Float, Int) = {
		val numPartititions = batchData.getNumPartitions
		val gePSModel: GEPSModel = gEModel.asInstanceOf[GEPSModel]
		val srcModel: RDD[(Int, HashMapModel)] = gePSModel.srcModel
		val dstModel: PSMatrix = gePSModel.dstModel

		val (batchLoss, batchCnt) = batchData.zipPartitions(srcModel)((iter1, iter2) => {
			var (startTime, endTime) = (0L, 0L)
			val pairsDataset = iter1.next()._2
			val srcIds = pairsDataset.src.toArray
			val dstIds = pairsDataset.dst.toArray
			val srcModel: HashMapModel = iter2.next()._2

			// get negative sampling nodes
			val random: Random = new Random(iterationId)
			val negArray: Array[Int] = Array.ofDim(negative)
			for (i <- 0 until (negArray.length)) {
				negArray(i) = random.nextInt(vertexNum)
			}

			// get dstModel indices to pull
			val indicesSet: IntOpenHashSet = new IntOpenHashSet()
			for(i <- 0 until(negArray.length)){
				indicesSet.add(negArray(i))
			}
			for(i <- 0 until(dstIds.length)){
				indicesSet.add(dstIds(i))
			}
			logInfo(s"*ghand*dstIds.length:${dstIds.length}")
			val indices: Array[Int] = indicesSet.toIntArray()

			startTime = System.currentTimeMillis()
			// pull them and train
			val result  = dstModel.psfGet(new GEPull(
				new GEPullParam(dstModel.id,
					indices,
					numNodePerRow,
					dimension)))
				.asInstanceOf[GEPullResult]

			endTime = System.currentTimeMillis()
			logInfo(s"*ghand*worker ${TaskContext.getPartitionId()} pulls ${indices.length} vectors " +
				s"from PS, takes ${(endTime - startTime) / 1000.0} seconds.")

			startTime = System.currentTimeMillis()
			val index2offset = new Int2IntOpenHashMap() //
			index2offset.defaultReturnValue(-1)
			for (i <- 0 until indices.length) index2offset.put(indices(i), i)
			val deltas = new Array[Float](result.layers.length)
			// deep copy for deltas, we do asgd, for shared Negative sampling, it is not asgd
			for(i <- 0 until(result.layers.length)){
				deltas(i) = result.layers(i)
			}
			var loss = 0.0f
			val sharedNegativeUpdate: Array[Float] = Array.ofDim(negative * dimension)
			for(i <- 0 until(srcIds.length)){
				val srcId = srcIds(i)
				val dstId = dstIds(i)
				val srcVec = srcModel.get(srcId)
				loss += psTrainOnePair(srcVec, dstId, negArray, sharedNegativeUpdate, index2offset, deltas)
			}

			for(i <- 0 until(deltas.length)){
				deltas(i) -= result.layers(i)
			}
			// update the shared negative sampling nodes
			for(i <- 0 until(negArray.length)){
				val ioffset = index2offset.get(negArray(i)) * dimension
				for(j <- 0 until(dimension)) {
					deltas(ioffset + j) += sharedNegativeUpdate(i * dimension + j)
				}
			}
			endTime = System.currentTimeMillis()
			// TODO: some penalty to frequently updates, i.e., stale updates
			logInfo(s"*ghand*worker ${TaskContext.getPartitionId()} finished training, " +
				s"takes ${(endTime - startTime) / 1000.0} seconds.")
			startTime = System.currentTimeMillis()
			dstModel.psfUpdate(new GEPush(
				new GEPushParam(dstModel.id,
					indices, deltas, numNodePerRow, dimension)))
				.get()
			endTime = System.currentTimeMillis()
			logInfo(s"*ghand*worker ${TaskContext.getPartitionId()} pushes ${indices.length} vectors " +
				s"to PS, takes ${(endTime - startTime) / 1000.0} seconds.")

			// push back and compute loss
			Iterator.single((loss, srcIds.length))
		}).reduce((x, y) => (x._1 + y._1, x._2 + y._2))


		logInfo(s"*ghand*batch finished, batchLoss: ${batchLoss / batchCnt}, batchCnt:${batchCnt}")
		(batchLoss, batchCnt)
	}

	/**
	  *
	  * @param srcVec
	  * @param dstId
	  * @param negArray
	  * @param sharedNegativeUpdate
	  * @param index2offset
	  * @param ctxVecs
	  * @return
	  */
	def psTrainOnePair(srcVec: Array[Float], dstId: Int, negArray: Array[Int],
					   sharedNegativeUpdate: Array[Float], index2offset: Int2IntOpenHashMap,
					   ctxVecs: Array[Float]): Float = {
//					   dstOffset: Int, negative: Int, sharedNegativeUpdate: Array[Float]): Float = {
		var loss: Float = 0.0f
		val srcUpdate: Array[Float] = Array.ofDim(dimension)

		// positive pair
		var label = 1
		val dstOffset = index2offset.get(dstId) * dimension
		var sum = 0.0f
		for(i <- 0 until(dimension)) {
			sum += srcVec(i) * ctxVecs(i + dstOffset)
		}
		val prob = FastMath.sigmoid(sum)
		val g = -(label - prob) * stepSize

		for (i <- 0 until (dimension)) {
			srcUpdate(i) -= g * ctxVecs(i + dstOffset)
			ctxVecs(i + dstOffset) -= g * srcVec(i)
		}
		loss += -FastMath.log(prob)

		// negative pairs
		for (negId <- 0 until (negArray.length)) {
			label = 0
			val dstOffset = index2offset.get(negArray(negId)) * dimension
			var sum = 0.0f
			for(i <- 0 until(dimension)){
				sum += srcVec(i) * ctxVecs(i + dstOffset)
			}
			val prob = FastMath.sigmoid(sum)
			val g = -(label - prob) * stepSize

			for (i <- 0 until (dimension)) {
				srcUpdate(i) -= g * ctxVecs(dstOffset + i)
				sharedNegativeUpdate(negId * dimension + i) -= g * srcVec(i)
			}
			loss += -FastMath.log(1 - prob)
		}

		for (i <- 0 until (srcVec.length)) {
			srcVec(i) += srcUpdate(i)
		}
		loss
	}
}
