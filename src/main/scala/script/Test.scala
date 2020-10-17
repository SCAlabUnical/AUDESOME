package script

import scala.io.Source

object Test {

  def main(args: Array[String]): Unit = {
    val lines = Source.fromFile("/home/emanuele/IdeaProjects/sparktest_2/src/main/scala/script/rome.txt").getLines.toArray
    lines.foreach(x=>println(x))
  }


}
