package com.cloudera.sa.simple

import parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import parquet.avro.AvroReadSupport

object SparkParquetReadExample {
  
  def main(args: Array[String]) {
    
    if (args.length < 2) {
      System.err.println("Usage: SparkParquetReadExample <master> <input-parquet>")
      System.exit(1)
    }
    
    val sc = new SparkContext(args(0), "ParquetReadExample", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    sc.addJar("target/spark-parquet-0.0.1-SNAPSHOT.jar")
    val job = new Job()
      
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[HotelTaxes]])
    
    val parquetData = sc.newAPIHadoopFile(args(1), classOf[ParquetInputFormat[HotelTaxes]], classOf[Void], classOf[HotelTaxes], job.getConfiguration)
    parquetData.take(10).foreach((tuple: Tuple2[Void, HotelTaxes]) => println {if (tuple._2 != null) tuple._2.getLocationName()})
  }
}
