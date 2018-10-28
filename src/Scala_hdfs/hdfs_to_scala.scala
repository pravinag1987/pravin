package Scala_hdfs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.databricks.spark.xml
import org.apache.spark.sql.functions.explode
//import org.apache.spark.sql.SparkSession.builder
import org.apache.spark.sql.functions.{col, lit, when}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
//import java.io.PrintWriter;
//import scala.sys.process._


object hdfs_to_scala {
  
   
 def main(args: Array[String]) = {
   
   val conf1 = new SparkConf()
      .setAppName("ReadHdfs")
      .setMaster("local")
    val sc = new SparkContext(conf1)
    
    val sql = new org.apache.spark.sql.SQLContext(sc)
     import sql.implicits._
   
   
   val filePath = "hdfs:///user/pravinag/python/source"
   
   val fileDestination = "hdfs:///user/pravinag/python/source/Processed_Process"
   
   val uri = "hdfs://localhost:9000"
   
    System.setProperty("HADOOP_USER_NAME", "user")
    val path = new Path(filePath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.listStatus(path)
    //os.foreach(x=> println(x.getPath))
    for(xx <- os)
    {
      //println(xx.getPath)
      
      val data = sc.textFile(xx.getPath.toString())
      
      data.foreach(println)
      
      val exists = fs.exists(new Path(fileDestination))
      
      println(exists)
      
      if (exists.toString() == "false")
      {
        fs.mkdirs(new Path(fileDestination))
      }
      
      val result = fs.rename(new Path(xx.getPath.toString()),
          new Path(fileDestination))
      

     
    }
    
    fs.close()
  
  
   //dd.write("hdfs://localhost:9000", "hdfs:///user/pravinag/python/source")

   
 }
  
}