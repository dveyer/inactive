import java.nio.file.{Files, Paths}

import utl.InitXMLData

import scala.sys._
import scala.util.control.Breaks.break

object MainApp {

  def main(args: Array[String]): Unit = {

    // Check program arguments block
    if (args.length != 1) {
      println(
        "Error: Cannot find program arguments.\n" +
          "Usage: spark-submit --class MainApp ./inactive.jar <config.xml>"
      )
      exit(-1)
    }

    if (!checkFile(args(0))) {
      println(s"Cannot read config file: $args(0)")
      exit(-1)
    }

    val initXMLData = new InitXMLData()
    val propMap: Map[String, String] = initXMLData.initGeneralAndFsData(args(0))
    val stgMap: Map[String, Boolean] = initXMLData.initStgMapAdv(args(0))
    val sparkMap: Map[String, String] = initXMLData.initSparkMap(args(0))
    val stgRunLevels = new StgRunLevels(propMap, sparkMap, stgMap)
    var levelStatus = true
    val stgLevels = propMap("stglevels")

    if (stgLevels != null) {
      var i: Integer = 0
      do {
        println(s"Start level $i...")
        if (!stgRunLevels.RunTask(String.valueOf(i))) {
          levelStatus = false
          break
          println(s"ERROR Level $i finished with Errors")
        }
        i += 1
      }
      while (i < Integer.parseInt(stgLevels))
    }

  }

  def checkFile(fn: String):Boolean = Files.exists(Paths.get(fn)) && Files.isReadable(Paths.get(fn))
}
