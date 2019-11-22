package ge

import java.util.Random

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import samplers.BaseSampler
import ge.basics.{HashMapModel, PairsDataset, DistributionMeta, PullKey}

class GESparkAnalysis(params: Map[String, String]) extends GraphEmbedding(params: Map[String, String]) {

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

		val numNet = dstKeys.map(x => {
			var cnt = 0
			val pullKeys = x._2.toIterator
			while(pullKeys.hasNext){
				val pullKey = pullKeys.next()
				cnt += pullKey.keys.size
			}
			cnt
		}).reduce((x, y) => x + y)

		logInfo(s"*ghand*number of dst vertices through network: ${numNet}")

		(0.0f, 1)
	}


}
