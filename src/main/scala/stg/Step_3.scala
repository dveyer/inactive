package stg

import utl.{SparkBase, Tools}
import org.apache.spark.sql.functions._

class Step_3(propMap: Map[String, String], sparkMap: Map[String, String], m:Integer, pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val sFolder = propMap("sfolder")   // Stagings folder
  val dsFolder = propMap("dsfolder")
  val P_DATE = tools.addDays(pdate,-5)
  val stg_name = this.getClass.getSimpleName + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._

  def get_ds_path(source:String, nm: String,i:Int, ext: String):String =
    dsFolder + "/" + source + "/" + nm + "/" + tools.patternToDate(tools.addMonths(tools.firstDay(P_DATE),i),"MM-yyyy") + "/" + ext
  def get_ref_ds_path(source:String, nm: String, ext: String):String = propMap("dsfolder") + "/" + source + "/" + nm + "/" + ext

  val fc_path = get_ref_ds_path("biis","fct_charge","*/")
  val df4_path = sFolder + "/" + "subs_base"
  val post_charge_path = sFolder +"/" + "post_charge"

  def RunJob():Boolean= {

    var status = true
    if (spark != null)
      try {

        val fc = spark.read.parquet(fc_path)
        val df4 = spark.read.parquet(df4_path)

        val df6 = fc
          .filter($"CREATION_DATE_KEY".cast(org.apache.spark.sql.types.DateType)>=tools.firstDay(tools.addDays(P_DATE,-130)) &&
            trunc($"PERIOD_COVERAGE_START_DATE","MM").cast(org.apache.spark.sql.types.DateType)>=tools.firstDay(tools.addDays(P_DATE,-130)) &&
            trunc($"PERIOD_COVERAGE_START_DATE","MM").cast(org.apache.spark.sql.types.DateType)<=tools.firstDay(P_DATE) &&
            $"INVOICE_TYPE_KEY".isin("B","BN","ET","UB"))
          .join(df4,fc("SUBS_KEY")===df4("SUBS_KEY") && fc("BAN_KEY")===df4("BAN_KEY"))
          .groupBy(
          df4("SUBS_KEY"),
          df4("BAN_KEY"),
          trunc(fc("PERIOD_COVERAGE_START_DATE"),"MM").as("PERIOD"))
          .agg(sum(fc("ACTIVITY_AMT")).as("ACTIVITY_AMT")).cache()

        var V_MON = tools.firstDay(tools.addDays(P_DATE,-130))

        while(V_MON < P_DATE)
        {
          var V_LD_2 = V_MON
          var PAR_D = tools.dateDiff(V_MON,tools.lastDay(V_MON))+1
          while(V_LD_2<tools.addMonths(V_MON,1))
          {
            tools.removeFolder(post_charge_path+"/"+tools.patternToDate(V_LD_2,"ddMMyyyy"))
            df6.filter($"PERIOD"===V_MON)
              .select(
              lit(V_LD_2).as("TIME_KEY"),
              $"SUBS_KEY",
              $"BAN_KEY",
              ($"ACTIVITY_AMT"/PAR_D).as("CHARGE_AMT"))
              .write.parquet(post_charge_path+"/"+tools.patternToDate(V_LD_2,"ddMMyyyy"))
            V_LD_2 = tools.addDays(V_LD_2,1)
          }
          V_MON = tools.addMonths(V_MON,1)
        }

        spark.close()
      } catch {
        case e: Exception => println(s"ERROR: $e")
          status = false
          if (spark != null) spark.close()
      } finally { }
    else status = false
    status
  }
}
