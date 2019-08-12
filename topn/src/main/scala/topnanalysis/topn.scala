package topnanalysis

import scala.util.matching.Regex
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.types.{StringType,StructField,IntegerType,StructType}
import scala.collection.mutable

/*
 * 
 * 
 * 
 * 
 * Class for Top N define methods :
 *              1. parse_apache_log_line(log_line: String) --> Accepts data from a RDD and parses it sends out Rows of parsed records
 *              2. getSchema():StructType --> returns a StructType of Schema for creating a DF
 *              3. calculateTopn(access_logs: DataFrame, sqlContext: SQLContext):DataFrame -->
 *                              Calculates TopN and Sends DF to the App
 * 
 * 
 */

class topn() extends Serializable  {
    
    def parse_apache_log_line(log_line: String):Row ={
     // Regular Expression that matches Apache Common log format 
     val keyValPattern: Regex = """^(\S+) (\S+) (\S+) \[(\S+?):\S+\s-\d{4}\] \"(\S+) (\S+) (\S+)\" (\d{3}) (\d+)""".r
    		 keyValPattern.findFirstMatchIn(log_line) match {
    		 case Some(i) => return Row(
    		        i.group(1),
                i.group(2),
                i.group(3),
                i.group(4),
                i.group(5),
                i.group(6),
                i.group(7),
                i.group(8).toInt,
                i.group(9))
    		 case None => {
    		   //accumulable += (log_line)
    		              Row("Not Matching")
    		   }
    	}
 }
  
  def getSchema():StructType ={
           return  StructType(
                            Seq(
                              StructField(name = "ip_address", dataType = StringType, nullable = false),
                              StructField(name = "client_identifier", dataType = StringType, nullable = false),
                              StructField(name = "username", dataType = StringType, nullable = false),
                              StructField(name = "date", dataType = StringType, nullable = false),
                              StructField(name = "HTTP_method", dataType = StringType, nullable = false),
                              StructField(name = "endpoint", dataType = StringType, nullable = false),
                              StructField(name = "protocol", dataType = StringType, nullable = false),
                              StructField(name = "response", dataType = IntegerType, nullable = false),
                              StructField(name = "size", dataType = StringType, nullable = false)
                            )
                          )
  }
  
  def calculateTopn(access_logs: DataFrame, sqlContext: SQLContext):DataFrame={
    
    access_logs.registerTempTable("common_logs")
    val TopN = sqlContext.sql("SELECT date,ip_address,endpoint, count(*) as freq FROM common_logs GROUP BY date,ip_address,endpoint ORDER BY count(*) desc")
     TopN
  }
  
  
  
  /*def writeFile(df: DataFrame, outfile: String){
    // Write function
    
  }*/
}