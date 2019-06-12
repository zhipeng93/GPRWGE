package ge

import java.util.Random

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import samplers.BaseSampler
import ge.basics.{HashMapModel, PairsDataset, DistributionMeta, PullKey}
import utils.FastMath

class GESpark(params: Map[String, String]) extends GraphEmbedding(params: Map[String, String]) {

	override def initModel(sampler: BaseSampler, bcMeta: Broadcast[DistributionMeta]): GEModel = {
		new GERDDModel(sampler, bcMeta, dimension)
	}

	override def train(batchData: RDD[(Int, PairsDataset)], bcMeta: Broadcast[DistributionMeta],
		gEModel: GEModel, vertexNum: Int, iterationId: Int): (Float, Int) = {
		val numPartititions = batchData.getNumPartitions
		val srcDstModel: RDD[(Int, (HashMapModel, HashMapModel))] = gEModel.asInstanceOf[GERDDModel].srcDstModel

		/* shared negative sampling */
		/* use the same seed to generate the negative IDs, to avoid broadcast-collect-broadcast scheduling delays*/
		/* compute the index of each dstModel needed by each partition */
		val dstKeys: RDD[(Int, Iterable[PullKey])] = batchData.mapPartitionsWithIndex((pid, iter) => {
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
			val random = new Random(iterationId)
			for(i <- 0 until(negative)){
				val dstId = random.nextInt(vertexNum)
				if(dstMeta(dstId) != pid){
					// this is where our partitioner works
					indexDstToPull(dstMeta(dstId)) += dstId
				}
			}
			indexDstToPull.map(x => new PullKey(pid, x)).zipWithIndex.map(x => (x._2, x._1)).toIterator
		}).groupByKey(numPartititions)

		/* get the dstModels from each server Id, organize them according to the partition id*/
		val dstModelForJoin: RDD[(Int, HashMapModel)] = dstKeys.zipPartitions(srcDstModel)((iter1, iter2) => {
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
		val tmp: RDD[(Float, Int, HashMapModel)] = srcDstModel
			.zipPartitions(batchData, dstModelForJoin)((iter1, iter2, iter3) => {
				val (mid, (srcModel, dstModel)) = iter1.next()
				val (did, pairsTrainset) = iter2.next()
				val (dstMid, netDstModel) = iter3.next()
				assert(mid == did && dstMid == mid)

				val negArray: Array[Int] = Array.ofDim(negative)
				val random = new Random(iterationId)
				for(i <- 0 until(negative)){
					negArray(i) = random.nextInt(vertexNum)
				}
				// clone is deepcopy
				val netDstModelBackup: HashMapModel = netDstModel.clone().asInstanceOf[HashMapModel]
				val sharedNegUpdate: Array[Array[Float]] = Array.ofDim(negative, dimension)
				for(i <- 0 until(negative)){
					sharedNegUpdate(i) = Array.ofDim(dimension)
				}

				val srcIds = pairsTrainset.src
				val dstIds = pairsTrainset.dst
				var loss = 0.0f
				for (i <- 0 until (srcIds.length)) {
					loss += trainOnePair(srcIds(i), dstIds(i), srcModel,
						dstModel, netDstModel, negArray, sharedNegUpdate)
				}
				// merge the sharedNegUpdate to dstModel and netDstModel
				val normalizer = srcIds.length
				FastMath.vecDotScalar(sharedNegUpdate, 1.0f / normalizer)
				for(i <- 0 until(negative)){
					val dstId = negArray(i)
					if(dstModel.containsKey(dstId)){
						dstModel.updateAdd(dstId, sharedNegUpdate(i))
					}
					else {
						/* it musy be in the netDstModel */
						netDstModel.updateAdd(dstId, sharedNegUpdate(i))
					}
				}
				// compute the delta of netDstModel
				val entryIterator = netDstModel.toEntryterator()
				while(entryIterator.hasNext){
					val entry = entryIterator.next()
					netDstModelBackup.updateSub(entry.getKey, entry.getValue)
				}
				Iterator.single((loss, srcIds.length, netDstModel))
			})

		// deal with the loss
		val (batchLoss, batchCnt): (Float, Int) = tmp.mapPartitions(iter => {
			// one element per partition
			val ele = iter.next()
			Iterator.single((ele._1, ele._2))
		}).reduce((x, y) => (x._1 + y._1, x._2 + y._2))

		// push the updates other workers
		// shuffle the update of contexts, make it happen.
		val updatedDstForJoin: RDD[(Int, HashMapModel)] = tmp.mapPartitions(iter => {
			val hashMapModel: HashMapModel = iter.next()._3
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
		srcDstModel.zipPartitions(updatedDstForJoin)((iter1, iter2) => {
			val (pid, (srcModel, dstModel)) = iter1.next()
			val (pid2, updatedModel) = iter2.next()
			assert(pid == pid2)
			val entryIter = updatedModel.toEntryterator()
			while(entryIter.hasNext){
				val entry = entryIter.next()
				val key = entry.getKey
				val value = entry.getValue
				// should add careful locking mechanism, as a PS control.
				dstModel.updateAdd(key, value)
			}
			Iterator.single(1)
		}).count()

		logInfo(s"*ghand*batch finished, batchLoss: ${batchLoss / batchCnt}, batchCnt:${batchCnt}")
		(batchLoss, batchCnt)
	}

	def trainOnePair(srcId: Int, dstId: Int, srcModel: HashMapModel,
						 dstModel: HashMapModel, netDstModel: HashMapModel,
						 negArray:Array[Int], sharedNegUpdate: Array[Array[Float]]): Float = {
		var loss: Float = 0.0f
		val srcUpdate: Array[Float] = Array.ofDim(dimension)
		val srcVec: Array[Float] = srcModel.get(srcId)

		// positive pair
		var label = 1
		var dstVec: Array[Float] = null
		if(dstModel.containsKey(dstId)){
			dstVec = dstModel.get(dstId)
		}
		else{
			/* it must be here */
			assert(netDstModel.containsKey(dstId))
			dstVec = netDstModel.get(dstId)
		}
		val prob = FastMath.sigmoid(FastMath.vecDotVec(srcVec, dstVec))
		val g = -(label - prob) * stepSize

		for (i <- 0 until (srcVec.length)) {
			srcUpdate(i) -= g * dstVec(i)
			dstVec(i) -= g * srcVec(i)
		}
		loss += -FastMath.log(prob)

		// negative pairs
		for (negCnt <- 0 until (negative)) {
			label = 0
			val negIndex = negArray(negCnt)
			var negVec: Array[Float] = null
			if(dstModel.containsKey(negIndex)){
				negVec = dstModel.get(negIndex)
			}
			else{
				/* it must be here */
				negVec = netDstModel.get(negIndex)
			}
			val prob = FastMath.sigmoid(FastMath.vecDotVec(srcVec, negVec))
			val g = -(label - prob) * stepSize

			for (i <- 0 until (srcVec.length)) {
				srcUpdate(i) -= g * negVec(i)
				sharedNegUpdate(negCnt)(i) -= g * srcVec(i)
			}
			loss += -FastMath.log(1 - prob)
		}

		for (i <- 0 until (srcVec.length)) {
			srcVec(i) += srcUpdate(i)
		}
		loss
	}
}
