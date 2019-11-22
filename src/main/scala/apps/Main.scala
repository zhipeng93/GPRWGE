package apps

import ge.{EmbeddingConf, GEPS, GESpark}
import utils.ArgsUtil

object Main{
	def main(args: Array[String]): Unit = {
		val params = ArgsUtil.parse(args)
		val platForm = params.getOrElse(EmbeddingConf.PLATFORM, "PS").toUpperCase()
		platForm match{
			case "SPARK" => new GESpark(params).run()
			case "PS" => new GEPS(params).run()
			case "SPARK_ANALYSIS" => new GESpark(params).run()
			case _ => new GESpark(params).run()
		}

	}
}