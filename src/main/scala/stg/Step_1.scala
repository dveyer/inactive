package stg

import utl.{SparkBase, Tools}
import org.apache.spark.sql.functions._

class Step_1(propMap: Map[String, String], sparkMap: Map[String, String], m:Integer, pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val sFolder = propMap("sfolder")   // Stagings folder
  val dsFolder = propMap("dsfolder")
  val P_DATE = pdate
  val stg_name = this.getClass.getSimpleName + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._

  def get_ref_ds_path(source:String, nm: String, ext: String):String = propMap("dsfolder") + "/" + source + "/" + nm + "/" + ext
  def get_ds_path(source:String, nm: String,i:Int, ext: String):String =
    dsFolder + "/" + source + "/" + nm + "/" + tools.patternToDate(tools.addMonths(tools.firstDay(P_DATE),i),"MM-yyyy") + "/" + ext

  //val df1_path_0 = get_ds_path("biis","fct_rtc_monthly",-1,"*.parq")
  val df1_path_1 = get_ds_path("biis","fct_rtc_monthly",-2,"*.parq")
  val df1_path_2 = get_ds_path("biis","fct_rtc_monthly",-3,"*.parq")
  val df2_path = get_ref_ds_path("biis","dim_subscriber","*.parq")
  val df3_path = get_ref_ds_path("biis","dim_price_plan","*.parq")
  val subs_base_path = sFolder + "/" + "subs_base"

  def RunJob():Boolean= {

    var status = true
    if (spark != null)
      try {

        val df1 = //spark.read.parquet(df1_path_0)
          //.union(
            spark.read.parquet(df1_path_1)
          .union(spark.read.parquet(df1_path_2))
        val df2 = spark.read.parquet(df2_path)
        val df3 = spark.read.parquet(df3_path)
        tools.removeFolder(subs_base_path)
        // Script initialization
        df1.filter(
          $"TIME_KEY".cast(org.apache.spark.sql.types.DateType)===tools.firstDay(tools.addMonths(P_DATE,-3)) && $"DW_STATUS_KEY".isin("A","S"))
          .join(df2,df1("SUBS_KEY")===df2("SUBS_KEY") && df1("BAN_KEY")===df2("BAN_KEY"))
          .join(df3,df2("CURR_PRICE_PLAN_KEY")===df3("PRICE_PLAN_KEY"))
          .select(
          df1("TIME_KEY"),
          trunc(df2("SUBS_ACTIVATION_DATE_KEY"),"MM").as("INFLOW_PERIOD"),
          df2("ACCOUNT_TYPE_KEY"),
          df2("PREPAID_IND"),
          df2("CURR_PRICE_PLAN_KEY"),
          when(df3("BUNDLE_GROUP").isNotNull,df3("BUNDLE_GROUP")).otherwise(
            when(df3("BUNDLE_GROUP").isNull,lit("OTHER")).otherwise(lit("OTHER"))).as("PP_GROUP_TYPE"),
          df1("SUBS_KEY"),
          df1("BAN_KEY"),
          df1("RTC_ACTIVE_IND"),
          df1("DW_STATUS_KEY")
        ).write.parquet(subs_base_path)
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