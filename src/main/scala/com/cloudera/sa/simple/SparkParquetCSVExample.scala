package com.cloudera.sa.simple

import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport, AvroReadSupport}
import parquet.filter.{RecordFilter, UnboundRecordFilter}
import java.lang.Iterable
import parquet.column.ColumnReader
import parquet.filter.ColumnRecordFilter._
import parquet.filter.ColumnPredicates._
import parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.hadoop.io.NullWritable
import scala.collection.mutable.ArrayBuffer

object SparkParquetCSVExample {
  
  def generateHotelTaxArrays( myList : List[List[String]] ) : Array[HotelTaxes] = {
     var z = new Array[HotelTaxes](myList.size) 
     var i =  0
    for (myEntry <- myList) {
            val ht = new HotelTaxes()
            ht.setTaxPayerNumber(myEntry(0).trim())
            ht.setTaxPayerName(myEntry(1).trim())
            ht.setTaxPayerAddress(myEntry(2).trim())
            ht.setTaxPayerCity(myEntry(3).trim())
            ht.setTaxPayerState(myEntry(4).trim())
            ht.setTaxPayerZipcode(myEntry(5).trim())
	    	ht.setTaxPayerCounty(myEntry(6).trim())
	    	ht.setOutletNumber(myEntry(7).trim().toInt)
	    	ht.setLocationName(myEntry(8).trim())
         	ht.setLocationAddress(myEntry(9).trim())
	    	ht.setLocationCity(myEntry(10).trim())
	    	ht.setLocationState(myEntry(11).trim())
	    	ht.setLocationZipcode(myEntry(12).trim().toInt)
	    	ht.setLocationCounty(myEntry(13).trim())
	    	ht.setLocationRoomCapacity(myEntry(14).trim().toInt)
	    	ht.setLocationRoomReceipts(myEntry(15).trim().toFloat)
	    	ht.setLocationTaxableReceipts(myEntry(16).trim().toFloat)
	    	z(i) = ht
	    	i = i+1
    }
     return  z
   }

  def main(args: Array[String]) {
    
    if (args.length < 3) {
      System.err.println("Usage: SparkParquetCSVExample <master> <input-csv> <output-folder>")
      System.exit(1)
    }
    
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "com.cloudera.sa.simple.SampleRegistrator")

    val sc = new SparkContext(args(0), "ParquetExample", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    sc.addJar("target/spark-parquet-0.0.1-SNAPSHOT.jar")
    val job = new Job()

    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
    AvroParquetOutputFormat.setSchema(job, HotelTaxes.SCHEMA$)
    
    // Configure the ParquetOutputFormat to use Avro as the serialization format
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    
    val lines = sc.textFile(args(1), 2)
    val rdd = lines.map(CSV.parse).flatMap(generateHotelTaxArrays).map(tax => (null, tax))
    

    // Save the RDD to a Parquet file
    rdd.saveAsNewAPIHadoopFile(args(2), classOf[Void], classOf[HotelTaxes],
      classOf[ParquetOutputFormat[HotelTaxes]], job.getConfiguration)
  }
}