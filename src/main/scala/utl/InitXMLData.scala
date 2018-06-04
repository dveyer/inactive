package utl

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import scala.xml.XML

class InitXMLData {

  def initGeneralAndFsData(fn: String):Map[String, String]={
    var genMap:Map[String, String] = Map.empty[String, String]
    val xml = XML.loadFile(fn)
    // **** Init General data ****
    val genNodes = (xml\\"init"\\"general").iterator
    while(genNodes.hasNext) {
      val node = genNodes.next()
      for(ss <- node.child) genMap += (ss.label -> ss.text)
    }
    // **** Init FS data ****
    val fsNodes = (xml\\"init"\\"fs").iterator
    while(fsNodes.hasNext) {
      val node = fsNodes.next()
      for(ss <- node.child) genMap += (ss.label -> ss.text)
    }
    genMap
  }

  def initDsSourceData(fn: String):Map[String, Boolean]={
    var dsMap:Map[String, Boolean] = Map.empty[String, Boolean]
    val xml = XML.loadFile(fn)
    val srcNodes = (xml\\"source"\\"ds").iterator
    val sf = (xml\\"source" \ "@sf").text
    val tf = (xml\\"source" \ "@tf").text
    val ext_source = (xml\\"source" \ "@ext_source").text
    val ext_dest = (xml\\"source" \ "@ext_dest").text
    while(srcNodes.hasNext) {
      val srcNode = srcNodes.next()
      val enableStatus = (srcNode \ "@enable").text.equals("1")
      dsMap += (srcNode.text + "|" + sf + "|" + tf + "|" + ext_source +"|" + ext_dest -> enableStatus)
    }
    dsMap
  }

  /*
  def initSourcesDailyMap(fn: String):Map[String, Boolean]= {
    var dsMap: Map[String, Boolean] = Map.empty[String, Boolean]
    val xml = XML.loadFile(fn)
    val srcNodes = (xml\\"sources"\\"ds").iterator
    val dsext = (xml\\"sources" \ "@dsext").text
    // *******************************************
    while(srcNodes.hasNext) {
      val srcNode = srcNodes.next()
      val enableStatus = (srcNode \ "@enable").text.equals("1")
      val source = (srcNode \ "@source").text
      val dstype = (srcNode \ "@dstype").text
      var dspath = source + "|" + dstype + "|" + srcNode.text + "|" + dsext
      dsMap += (dspath -> enableStatus)
    }
    dsMap
  }
  def initSourcesDailyMap(fn: String):Map[String, Boolean]= {
    var dsMap: Map[String, Boolean] = Map.empty[String, Boolean]
    val xml = XML.loadFile(fn)
    val srcNodes = (xml\\"sources"\\"ds").iterator
    val dsext = (xml\\"sources" \ "@dsext").text
    // *******************************************
    while(srcNodes.hasNext) {
      val srcNode = srcNodes.next()
      val enableStatus = (srcNode \ "@enable").text.equals("1")
      val source = (srcNode \ "@source").text
      val dstype = (srcNode \ "@dstype").text
      if (dstype.equals("daily")) {
        val actual = (srcNode \ "@actual").text
        dsMap += (source + "|" + dstype + "|" + srcNode.text + "|" + dsext + "|" + actual -> enableStatus)
      }
      else dsMap += (source + "|" + dstype + "|" + srcNode.text + "|" + dsext -> enableStatus)
    }
    dsMap
  }
  */

  def initStgMapAdv(fn:String):Map[String, Boolean]={
    var stgMap:Map[String, Boolean] = Map.empty[String, Boolean]
    val xml = XML.loadFile(fn)
    val stgNodes = (xml\\"stgmap"\\"stg").iterator
    while(stgNodes.hasNext) {
      val stgNode = stgNodes.next()
      val enableStatus = (stgNode \ "id" \ "@status").text.equals("1")
      val period = (stgNode \ "id" \ "@period").text
      val pdate = (stgNode \ "id" \ "@pdate").text
      //val scale = (stgNode \ "id" \ "@scale").text
      val level = (stgNode \ "id" \ "@level").text
      //stgMap += ((stgNode \ "id").text+"|"+period+"|"+pdate+"|"+scale+"|"+level -> enableStatus)
      stgMap += ((stgNode \ "id").text+"|"+period+"|"+pdate+"|"+level -> enableStatus)
    }
    stgMap
  }

  def initSparkMap(fn: String):Map[String, String]={
    var sparkMap:Map[String, String] = Map.empty[String, String]
    val xml = XML.loadFile(fn)
    val genNodes = (xml\\"init"\\"spark").iterator
    while(genNodes.hasNext) {
      val node = genNodes.next()
      for(ss <- node.child) sparkMap += (ss.label -> ss.text)
    }
    sparkMap
  }

  def initHiveMap(fn:String):Map[String, String]={
    var hiveMap:Map[String, String] = Map.empty[String, String]
    val xml = XML.loadFile(fn)
    // **** Init Hive data ****
    val genNodes = (xml\\"init"\\"hive").iterator
    while(genNodes.hasNext) {
      val node = genNodes.next()
      for(ss <- node.child) hiveMap += (ss.label -> ss.text)
    }
    hiveMap
  }

  def getSysDate(fmt: String): String = LocalDateTime.now(ZoneId.of("Asia/Almaty")).format(DateTimeFormatter.ofPattern(fmt))

}
