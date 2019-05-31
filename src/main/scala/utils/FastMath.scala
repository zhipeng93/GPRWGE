package utils

object FastMath {

	private final val MAX_SIGMOID = 8
	private final val SIGMOID_TABLE_SIZE = 1024
	private final val LOG_TABLE_SIZE = 1024
	private final val logTable = {
		Array.tabulate(LOG_TABLE_SIZE + 1)(i =>
			math.log((i + 1e-5) / LOG_TABLE_SIZE).toFloat
		)
	}
	private final val sigmoidTable = {
		Array.tabulate(SIGMOID_TABLE_SIZE + 1)(i => {
			val x = (i * 2 * MAX_SIGMOID).toDouble / SIGMOID_TABLE_SIZE - MAX_SIGMOID
			1.0f / (1.0f + math.exp(-x).toFloat)
		})
	}

	def sigmoid(x: Float): Float = {
		if (x < -MAX_SIGMOID)
			0.0f
		else if (x > MAX_SIGMOID)
			1.0f
		else
			sigmoidTable(((x + MAX_SIGMOID) * SIGMOID_TABLE_SIZE / MAX_SIGMOID / 2).toInt)
	}

	def log(x: Float): Float = {
		if (x > 1.0) 0.0f else logTable((x * LOG_TABLE_SIZE).toInt)
	}

	def vecDotVec(arr1: Array[Float], arr2: Array[Float]): Float = {
		var sum = 0.0f
		for(i <- 0 until(arr1.length)){
			sum += arr1(i) * arr2(i)
		}
		sum
	}

	def vecDotScalar(array: Array[Array[Float]], scalar: Float): Array[Array[Float]] = {
		for(i <- 0 until(array.length)){
			for(j <- 0 until(array(i).length)) {
				array(i)(j) *= scalar
			}
		}
		array
	}

	def vecAdd(arr1: Array[Array[Float]], arr2: Array[Array[Float]]): Array[Array[Float]] = {
		val arr3: Array[Array[Float]] = Array.ofDim(arr1.length, arr1(0).length)
		for(i <- 0 until(arr1.length)){
			for(j <- 0 until arr1(i).length){
				arr3(i)(j) = arr1(i)(j) + arr2(i)(j)
			}
		}
		arr3
	}

	/**
	  * inplace add. Result stored in
	  * @param arr1
	  * @param arr2
	  * @return
	  */
	def inPlaceVecAdd(arr1: Array[Array[Float]], arr2: Array[Array[Float]]): Array[Array[Float]] = {
		for(i <- 0 until(arr1.length)){
			for(j <- 0 until arr1(i).length){
				arr1(i)(j) += arr2(i)(j)
			}
		}
		arr1
	}
}