package topnanalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.types.{StringType,StructField,IntegerType,StructType}
import scala.collection.mutable

/*
 * 
 * Top N application programs which takes 2 paths as Input 
 *     args(0) for Input
 *     args(1) for output path to write results
 *     
 *     Step 1: Read the input into a RDD
 *     step 2: Apply RegEx in function 'parse_apache_log_line' to parse the fields and convert into ROW[]
 *     Step 3: create DataFrame from getSchema() and the Rows from Step 2 and create a tempTable
 *     Step 4: Write results to a Output file args(1)
 * 
 */

object topNapp {
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("Top N stats")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val accumulable = sc.accumulableCollection(mutable.HashSet[(Any)]())
    
    if (args.length < 2) {
					println("Usage:  <input> <output>")
					System.exit(1)
				}
    
    val topnObj = new topn()
    val rawData = sc.textFile(args(0))
    val dataDF = rawData.map(topnObj.parse_apache_log_line).cache()
    val access_logs =   sqlContext.createDataFrame(dataDF.filter(!_.getString(0).contains("Not Matching")), topnObj.getSchema())
    val TopN = topnObj.calculateTopn(access_logs, sqlContext)
    TopN.rdd.saveAsTextFile(args(1))
  
  }
}