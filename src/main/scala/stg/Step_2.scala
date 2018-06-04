package stg

import utl.{SparkBase, Tools}
import scala.util.control.Breaks.break
import org.apache.spark.sql.functions._

class Step_2(propMap: Map[String, String], sparkMap: Map[String, String], m:Integer, pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val sFolder = propMap("sfolder")   // Stagings folder
  val dsFolder = propMap("dsfolder")
  val P_DATE = tools.addDays(pdate,-3)
  val stg_name = this.getClass.getSimpleName + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._

  val dl_path = "/dmp/daily_stg/tg_det_layer_trans_parq" + "/"
  val df4_path = sFolder + "/" + "subs_base"

  def RunJob():Boolean= {
    var status = true
    var i:Integer = 0
    do {
        if (!RunTask(sFolder + "/" + "subs_activity" + "/" + tools.patternToDate(tools.addDays(P_DATE,-i),"ddMMyyyy"), -i))
      {
        status = false
        break
      }
      i += 1
    }
    while(i<m) // for tests it is set to 10, actually it equals 130
    spark.close()
    status
  }

  def RunTask(stg_path: String, i: Integer):Boolean= {
    var status = true
    if (spark != null)
      try {
        val dl = spark.read.parquet(dl_path + tools.patternToDate(tools.addDays(P_DATE,i),"ddMMyyyy"))
        val df4 = spark.read.parquet(df4_path)
        tools.removeFolder(stg_path)
        // Script initialization
        dl.filter($"SRC".isin("CHA","PSA","GPRS","POST","ROAM","CHR"))
            //$"CALL_START_TIME".cast(org.apache.spark.sql.types.DateType)>=tools.addDays(P_DATE,-1) &&
            //$"CALL_START_TIME".cast(org.apache.spark.sql.types.DateType)<P_DATE)
          .join(df4, dl("SUBS_KEY")===df4("SUBS_KEY") && dl("BAN_KEY")===df4("BAN_KEY"))
          .groupBy(
          dl("CALL_START_TIME").cast(org.apache.spark.sql.types.DateType).as("PERIOD"),
          dl("SUBS_KEY"),
          dl("BAN_KEY"))
          .agg(
          sum(when(dl("CALL_TYPE_CODE")==="V" && dl("SRC").isin("CHA","PSA","GPRS","POST","ROAM") && dl("ACTUAL_CALL_DURATION_SEC")>0 &&
            dl("COUNTED_CDR_IND")===1,dl("ACTUAL_CALL_DURATION_SEC")/60).otherwise(0)).as("VOICE_DUR_MIN"),
          sum(when(dl("CALL_TYPE_CODE")==="G" && dl("SRC").isin("CHA","PSA","GPRS","POST","ROAM") && dl("ROUNDED_DATA_VOLUME")>0 &&
            dl("COUNTED_CDR_IND")===1,dl("ROUNDED_DATA_VOLUME")/1024/1024).otherwise(0)).as("DATA_VLM_MB"),
          sum(when(dl("CALL_TYPE_CODE")==="S" && dl("SRC").isin("CHA","PSA","GPRS","POST","ROAM") && dl("CALL_DIRECTION_IND")===2 &&
            dl("COUNTED_CDR_IND")===1,1).otherwise(0)).as("OUT_SMS_AMT"),
          sum(when(dl("CALL_TYPE_CODE")==="M" && dl("SRC").isin("CHA","PSA","GPRS","POST","ROAM") && dl("CALL_DIRECTION_IND")===2 &&
            dl("COUNTED_CDR_IND")===1,1).otherwise(0)).as("OUT_MMS_AMT"),
          sum(when(dl("SRC").isin("CHA","PSA","GPRS","POST","ROAM","CHR"),$"CHARGE_AMT").otherwise(0)).as("CHARGE_AMT")
        )
          .write.parquet(stg_path)

      } catch {
    case e: Exception => println(s"ERROR: $e")
      status = false
      RunTask(sFolder + "/" + "subs_activity" + "/" + tools.patternToDate(tools.addDays(P_DATE,-i),"ddMMyyyy"), -i-1)
      if (spark != null) spark.close()
  } finally { }
  else status = false
  status
  }

}