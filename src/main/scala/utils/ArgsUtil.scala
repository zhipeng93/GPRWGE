package utils
import scala.collection.mutable

object ArgsUtil {

	def parse(args: Array[String]): Map[String, String] = {
		val cmdArgs = new mutable.HashMap[String, String]()
		println("parsing parameter")
		for (arg <- args) {
			val sepIdx = arg.indexOf(":")
			if (sepIdx != -1) {
				val k = arg.substring(0, sepIdx).trim
				val v = arg.substring(sepIdx + 1).trim
				if (v != "" && v != "Nan" && v != null) {
					cmdArgs.put(k.toUpperCase(), v)
					println(s"param $k = $v")
				}
			}
		}
		cmdArgs.toMap
	}
}