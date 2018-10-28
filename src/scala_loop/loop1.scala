package scala_loop


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.databricks.spark.xml
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.SparkSession.builder
import org.apache.spark.sql.functions.{col, lit, when}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem



 //import com.mongodb.spark.config.WriteConfig
 
 //import com.mongodb.spark.MongoSpark
 
 import org.bson.Document;


//import com.mongodb.spark.notNull


 //import com.mongodb.spark._

//import scala.collection.JavaConverters._

import com.mongodb.spark._
import com.mongodb.spark.sql._
import com.mongodb.spark.config._

import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.mongodb.ConnectionString

//import org.apache.commons.io.IOUtils


//import scalax.file.Path
//import scalax.file.FileOps
//import scala.reflect.io.File

//import com.stratio.provider.
//import com.stratio.provider.mongodb.
//import com.stratio.provider.mongodb.schema.
//import com.stratio.provider.mongodb.writer.
//import MongodbCon

object loop1 {
  
  def main(args: Array[String]) = {
    
     val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
    val sql = new org.apache.spark.sql.SQLContext(sc)
     import sql.implicits._
     
     val path = "file:///home/hduser1/PravinFiles/MongoDB/Adventurework_data/csvfile/goodFormat/Test1/"
     val confff = new Configuration()
     val fs = FileSystem.get(confff)
     val p = new Path(path)
    
     val ls = fs.listStatus(p)
     
   
     
     ls.foreach( x => {
       
       val f = x.getPath.toString
       
       val line = sc.textFile(f).mapPartitionsWithIndex{(x,y) => if(x==0) y.drop(1) else y}
       
       val line1 = line.map(x => x.split(","))
       
       val line2 = line1.map(x => (x(0).toInt, x(1).toString, x(2).toString, x(3).toString)).collect()
       
       val line3 = sc.parallelize(line2).toDF("DepartmentID", "Name","GroupName","ModifiedDate")
       
       
       MongoSpark.save(line3, WriteConfig(Map("uri" -> "mongodb://127.0.0.1/Test.HumanResources_Department")))
       
       
       val dest = new Path("file:///home/hduser1/PravinFiles/MongoDB/Adventurework_data/csvfile/goodFormat/Test1/Archive")
       
    
       
    if(fs.exists(dest))
    {
      

      fs.moveToLocalFile(new Path(f), 
            new Path("file:///home/hduser1/PravinFiles/MongoDB/Adventurework_data/csvfile/goodFormat/Archive/"))
   }
    else
   {
    
      fs.mkdirs(new Path("file:///home/hduser1/PravinFiles/MongoDB/Adventurework_data/csvfile/goodFormat/Archive"))
      
       
          fs.moveToLocalFile(new Path(f), 
          new Path("file:///home/hduser1/PravinFiles/MongoDB/Adventurework_data/csvfile/goodFormat/Archive/"))
    }
       

 })
     


  }
  
}