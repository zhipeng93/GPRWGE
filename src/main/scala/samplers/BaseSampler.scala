package samplers

import org.apache.spark.rdd.RDD
import ge.basics.{ChunkDataset, DistributionMeta, PairsDataset}
import java.util.Random

import org.apache.spark.internal.Logging

/**
 * base class for samplers of different random walk based models.
 * Functions include:
 * (1) take corpus as RDD[Array[Int]], output batch of vertex pairs
 * (2) approximate the ratio of each node serve as a "word" or "context"
 * (3) simple graphs like one-way-graphs, or the graph partitions.
 * *  Note that when sampling word-context pairs, we do not incur communication.
 */

abstract class BaseSampler(val trainset: RDD[(ChunkDataset, Int)], val vertexNum: Int) extends Serializable with Logging{

    /**
      * generate a batch of training data given a corpus of training data
      * sizeof(one chunk) = batch size.
      * @return postive vertex pairs
      */
    def batchIterator(): Iterator[RDD[PairsDataset]] = {
        new Iterator[RDD[PairsDataset]] with Serializable {
            override def hasNext(): Boolean = true

            // generate one batch of training data stored in RDD.
            // one element in each partition.
            override def next(): RDD[PairsDataset] = {
                trainset.mapPartitions(iter => {
                    var numChunks = -1
                    var chunkDataset: ChunkDataset = null
                    if(iter.hasNext) {
                        val ele = iter.next()
                        chunkDataset = ele._1
                        numChunks = ele._2
                    }
                    var chunkIndex = new Random().nextInt(numChunks)
                    while(chunkIndex > 0){
                        chunkIndex -= 1
                        chunkDataset = iter.next()._1
                    }
                    Iterator.single(generatePairs(chunkDataset))
                })
            }
        }
    }

    /**
      * output the positive word-context pairs
      * @param chunkDataset
      * @return
      */
    def generatePairs(chunkDataset: ChunkDataset): PairsDataset

    /**
      * compute the partitionId of src and dst according to each method given the #partitions
      *     (1) distribute the source vertex for load balance,
      *     (2) distribute the destination vertex for reduce the communication cost.
      *     src[i] = pid, vertex $i$'s word embedding is on partition $pid$
      */
    def hashDestinationForNodes(numPartitions: Int): DistributionMeta = {
        val src: Array[Int] = Array.ofDim(vertexNum)
        val dst: Array[Int] = Array.ofDim(vertexNum)
        for(i <- 0 until(vertexNum)){
            src(i) = i % numPartitions
            dst(i) = i % numPartitions
        }

        new DistributionMeta(src, dst)
    }

    /**
      * subclass should override this method.
      * This is the core contribution of this work.
      * may split one vertex. Could be very similar to existing graph partition methods.
      * @param numPartitions
      * @return
      */
    def destinationForNodes(numPartitions: Int): DistributionMeta = {
        hashDestinationForNodes(numPartitions)
    }


}
