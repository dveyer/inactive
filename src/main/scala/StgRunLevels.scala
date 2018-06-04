import utl.Tools

class StgRunLevels(propMap: Map[String, String], sparkMap: Map[String, String], stgMap:Map[String, Boolean]) {

  val tools = new Tools("Asia/Almaty", "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", propMap)
  var stg_status:Map[String, Boolean] = Map.empty[String, Boolean]

  def RunTask(n:String):Boolean = {
    var status_level = true

    for((stg,runStatus) <- stgMap) {

      val m: Int = stg.split("\\|")(1).toInt
      val pdate = tools.getDateFromXml(stg.split("\\|")(2))
      val level = stg.split("\\|")(3)

      if (runStatus && level.equals(n)) {

        val stgClass = Class.forName(stg.split("\\|")(0))
        val stgConstructor = stgClass.getConstructor(
          classOf[scala.collection.immutable.Map[String, String]],
          classOf[scala.collection.immutable.Map[String, String]],
          classOf[Integer],
          classOf[String],
          tools.getClass
        )
        println("Running: " + stg.split("\\|")(0))
        val stgRunJob = stgConstructor.newInstance(propMap, sparkMap, m: Integer, pdate, tools).getClass.getMethod("RunJob")
        stg_status += (stg.split("\\|")(0) ->
          stgRunJob
            .invoke(stgConstructor.newInstance(propMap, sparkMap, m: Integer, pdate, tools))
            .asInstanceOf[Boolean])
      }
    }
    // Check stagings result
    for ((stg, status) <- stg_status) if (!status) status_level = false
    status_level
  }

}
