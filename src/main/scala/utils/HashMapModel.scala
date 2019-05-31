package utils

import java.util.Map.Entry
import java.util.{HashMap => JHashMap}

// not consecutive vertexId
class HashMapModel() extends Serializable {
	val model: JHashMap[Int, Array[Float]] = new JHashMap[Int, Array[Float]]()
	def put(key: Int, value: Array[Float]): Unit ={
		model.put(key, value)
	}

	def get(key: Int): Array[Float] = {
		model.get(key)
	}

	def containsKey(key: Int): Boolean = {
		model.containsKey(key)
	}

	def toEntryterator(): java.util.Iterator[Entry[Int, Array[Float]]] = {
		model.entrySet().iterator()
	}

	def updateAdd(key: Int, delta: Array[Float]): Unit ={
		// we incur in-place add
		val arr = get(key)
		for(i <- 0 until(arr.length))
			arr(i) += delta(i)
	}

	override def toString: String = model.toString
}