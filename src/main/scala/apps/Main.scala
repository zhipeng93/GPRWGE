package apps

import ge.GraphEmbedding
import utils.ArgsUtil

object Main{
	def main(args: Array[String]): Unit = {
		new GraphEmbedding(ArgsUtil.parse(args)).run()
	}
}