package com.stc.FreshArchiver
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.joda.time._
import org.joda.time.format._
import org.apache.spark.sql.SparkSession
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.functions._


object FreshArchiver {

  def main(args:Array[String]){
  val spark = SparkSession.builder().appName("AggregationArchiver").enableHiveSupport().getOrCreate()
       
        println(" Spark session started") 
        
    //val kuduMaster="hddlm1.stc.com.bh:7051,hddlm2.stc.com.bh:7051,hddlm3.stc.com.bh:7051"
    val kuduMaster=args(0)
    val etltbl =args(1)
    val tempTable=args(2)
    
    
    val df_table_read = spark.read.options(Map("kudu.master" -> kuduMaster, "kudu.table" ->etltbl)).format("org.apache.kudu.spark.kudu").load()
    
    val filteredDF=df_table_read.filter(df_table_read("pipelineid")==="RequestWebsite_test")
    
    val modifiedDF=filteredDF.withColumn("pipeline_lastrundate",filteredDF("pipeline_lastrundate").cast(DateType))
    .withColumn("pipeline_nextrundate",filteredDF("pipeline_nextrundate").cast(DateType))
    
    val newDF = modifiedDF.withColumn("pipeline_nextrundate",modifiedDF("pipeline_nextrundate").cast(StringType))
    .withColumn("pipeline_lastrundate",modifiedDF("pipeline_lastrundate").cast(StringType))
    
    val startdt=newDF.rdd.map{r=>(r.getAs[String]("pipeline_lastrundate"),r.getAs[String]("pipeline_nextrundate"))}
  
    val start_new=startdt.map(r=>r._1).collect
    val end_new=startdt.map(r=>r._2).collect
    
    val startdate=start_new(0)
    val enddate=end_new(0)
    
    val kuduContext = new KuduContext(kuduMaster,spark.sparkContext)
    
    
    val start_time=DateTime.parse(startdate, DateTimeFormat.forPattern("yyyy-MM-dd"))
    val end_time=DateTime.parse(enddate, DateTimeFormat.forPattern("yyyy-MM-dd"))
    val time_format="yyyy-MM-dd"
  
    val tableFolder = tempTable match {
          
          case "aggregation_websitedetails_new" => "Aggregated_Website_new" 
          
        }
    
        for(i <- 0 to Days.daysBetween(start_time, end_time).getDays()){
                 
       var processDate_ = start_time.plusDays(i)
        var processEndate_ = processDate_.plusDays(1)
        
        var processDate = processDate_.toString(time_format)
        var processEndate = processEndate_.toString(time_format)
       
         var partitionDate = processDate_.toString("yyyy-MM-dd")
        
        //val df = spark.read.format("avro").load("/user/prodbigdataserviceac/smartcare/RequestWebsite/event_date="+ processDate+ "/sdc*","/user/prodbigdataserviceac/smartcare/RequestWebsite/event_date="+processEndate+"/sdc*")
        
        val df = spark.read.format("avro").load("/user/prodbigdataserviceac/smartcare/RequestWebsite/event_date="+ processDate+ "/sdc*")
        
        val dataDF = df.createOrReplaceTempView(s"$tempTable")
        val queryDF =spark.sqlContext.sql(s"select to_date(time) time,msisdn,case when lower(dns_domain) like '%facebook%'        then 'Facebook'  when lower(dns_domain) like '%google%' then 'Google' when lower(dns_domain) like '%youtube%' then 'YouTube' when lower(dns_domain) like '%instagram%' then 'Instagram' when lower(dns_domain) like '%arpa%' then 'Arpa' when lower(dns_domain) like '%apple%' then 'Apple' when lower(dns_domain) like '%microsoft%' then 'Microsoft' else lower(dns_domain) end as social_account,count(*) from $tempTable group by to_date(time),msisdn,dns_domain")
        
        queryDF.withColumn("time",col("time").cast(TimestampType)).withColumnRenamed("count(1)","count").coalesce(1).write.format("PARQUET").option("compression", "snappy").mode("overwrite").save("/user/prodbigdataserviceac/smartcare/" + tableFolder + "/event_date=" + partitionDate);
        
        }
        
  }
}