package stg

import utl.{SparkBase, Tools}
import org.apache.spark.sql.functions._

class Step_4(propMap: Map[String, String], sparkMap: Map[String, String], m:Integer, pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val sFolder = propMap("sfolder")   // Stagings folder
  val dsFolder = propMap("dsfolder")
  val P_DATE = tools.addDays(pdate,-5)
  val stg_name = this.getClass.getSimpleName + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._

  val df7_path = sFolder + "/" + "post_charge" + "/*/"
  val subs_activity_daily_path = sFolder + "/" + "subs_activity_daily"
  val daily_inc_path = sFolder + "/" + "daily_inc"
  val df4_path = sFolder +"/" + "subs_base"

  def RunJob():Boolean= {

    var status = true
    if (spark != null)
      try {

        val df7 = spark.read.parquet(df7_path)
        val df4 = spark.read.parquet(df4_path)
        var sa = spark.read.parquet(sFolder + "/" + "subs_activity" + "/" + tools.patternToDate(tools.addDays(P_DATE,0),"ddMMyyyy"))
        var i = 1
        while (i<130){
          sa = sa.union(spark.read.parquet(sFolder + "/" + "subs_activity" + "/" + tools.patternToDate(tools.addDays(P_DATE,-i),"ddMMyyyy")))
          i=i+1
        }

        tools.removeFolder(subs_activity_daily_path)
        tools.removeFolder(daily_inc_path)

        val df8 = sa.select(
          $"PERIOD",
          $"SUBS_KEY",
          $"BAN_KEY",
          $"VOICE_DUR_MIN",
          $"DATA_VLM_MB",
          $"OUT_SMS_AMT",
          $"OUT_MMS_AMT",
          $"CHARGE_AMT"
        ).union(
          df7.select(
            $"TIME_KEY".as("PERIOD"),
            $"SUBS_KEY",
            $"BAN_KEY",
            lit(0).as("VOICE_DUR_MIN"),
            lit(0).as("DATA_VLM_MB"),
            lit(0).as("OUT_SMS_AMT"),
            lit(0).as("OUT_MMS_AMT"),
            $"CHARGE_AMT" )
        ).cache()

        val df9 =
        df8.groupBy(
          $"PERIOD",
          $"SUBS_KEY",
          $"BAN_KEY").agg(
          sum($"VOICE_DUR_MIN").as("VOICE_DUR_MIN"),
          sum($"DATA_VLM_MB").as("DATA_VLM_MB"),
          sum($"OUT_SMS_AMT").as("OUT_SMS_AMT"),
          sum($"OUT_MMS_AMT").as("OUT_MMS_AMT"),
          sum($"CHARGE_AMT").as("CHARGE_AMT")
        )
          .cache()

        var SPC_DATE_4 = tools.addDays(P_DATE,-130)

        while (SPC_DATE_4 <= P_DATE)
        {
          if (!tools.checkExistFolder(daily_inc_path))
          {
            df4.join(
              df9,df4("subs_key")===df9("subs_key") &&
                df4("ban_key")===df9("ban_key") &&
                df9("period").cast(org.apache.spark.sql.types.DateType)===SPC_DATE_4,"left")
              .select(
                lit(SPC_DATE_4).as("period"),
                df4("subs_key"),
                df4("ban_key"),
                when((df9("data_vlm_mb")+df9("voice_dur_min")+df9("out_mms_amt")+df9("out_sms_amt")+df9("charge_amt")) > 0, 0).otherwise(1).as("ind")
              )
              .write.parquet(daily_inc_path + "/" + tools.patternToDate(SPC_DATE_4,"ddMMyyyy"))
          }
          else
          {
            val tr1 = spark.read.parquet(daily_inc_path + "/*/")
            df4.join(df9,
              df4("subs_key")===df9("subs_key") &&
                df4("ban_key")===df9("ban_key") &&
                df9("period").cast(org.apache.spark.sql.types.DateType)===SPC_DATE_4,"left").join(
              tr1,df4("subs_key")===tr1("subs_key") &&
                df4("ban_key")===tr1("ban_key") &&
                tr1("period").cast(org.apache.spark.sql.types.DateType)===tools.addDays(SPC_DATE_4,-1),"left").select(
              lit(SPC_DATE_4).as("period"),
              df4("subs_key"),
              df4("ban_key"),
              ((lit(1)+when(tr1("ind").isNull,0).otherwise(tr1("ind")).cast(org.apache.spark.sql.types.IntegerType))*
                when((df9("data_vlm_mb")+df9("voice_dur_min")+df9("out_mms_amt")+df9("out_sms_amt")+df9("charge_amt")) > 0, 0).otherwise(1)).as("ind")
            ).write.parquet(daily_inc_path + "/" + tools.patternToDate(SPC_DATE_4,"ddMMyyyy"))
          }
          SPC_DATE_4 = tools.addDays(SPC_DATE_4,1)
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
