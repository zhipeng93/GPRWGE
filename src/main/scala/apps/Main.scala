package apps

import ge.{GEPS, GESpark, EmbeddingConf}
import utils.ArgsUtil

object Main{
	def main(args: Array[String]): Unit = {
		val params = ArgsUtil.parse(args)
		val platForm = params.getOrElse(EmbeddingConf.PLATFORM, "PS").toUpperCase()
		platForm match{
			case "SPARK" => new GESpark(params).run()
			case "PS" => new GEPS(params).run()
			case _ => new GESpark(params).run()
		}

	}
}