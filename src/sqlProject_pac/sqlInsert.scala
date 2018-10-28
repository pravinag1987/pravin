package sqlProject_pac

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.databricks.spark.xml
import org.apache.spark.sql.functions.explode
//import org.apache.spark.sql.SparkSession.builder
import org.apache.spark.sql.functions.{col, lit, when}




object sqlInsert {
  
  def main(args: Array[String]) = {
    
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
    val sql = new org.apache.spark.sql.SQLContext(sc)
     import sql.implicits._
    
    
    val readxml = sql.read.format("com.databricks.spark.xml").option("rowTag","NewDataSet")
    .load("file:///home/hduser1/PravinFiles/Sparkfiles/AtlasCapco/Master/Avg_Master_Feed_Pressure.xml")
    
    val col = readxml.withColumn("data",explode($"Avg_Master_Feed_Pressure"))
    
    val sele = col.select($"data.AvgUnit").na.drop(how = "any")
    
    val sele1 = sele.withColumn("IsActive", lit(1))
    
    val df = sql.read.format("jdbc").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("user","SA").option("password", "login@123")
    .option("url", "jdbc:sqlserver://pravin-Lenovo-ideapad-110-15ISK\\SQLFULL:1433;databaseName=BitPerformance")
    .option("dbTable", "tblAvgUnitMaster").load()
    
    val df1 = sele1.write.mode("append").format("jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("user","SA").option("password", "login@123")
    .option("url", "jdbc:sqlserver://pravin-Lenovo-ideapad-110-15ISK\\SQLFULL:1433;databaseName=BitPerformance")
    .option("dbTable", "tblAvgUnitMaster").option("partitionColumn", "AvgUnit")
    .option("partitionColumn","IsActive").option("lowerBound", "1").option("upperBound", "1")
    .option("numPartitions", "1").save()
    
    
    
    
  }
  
}