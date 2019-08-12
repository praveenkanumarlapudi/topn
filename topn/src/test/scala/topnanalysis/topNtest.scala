package topnanalysis

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{SharedSparkContext, DataFrameSuiteBase}
import org.apache.spark.sql.{SQLContext, Row, DataFrame}

/*
 * Spark Test suite for Topn Application testing, class extends DataFrameSuiteBase since it is 
 * it requries SparkContext and SQLContext successfult testing
 * 
 * data is from "sample.txt" which contains some common logs with pre calculated results to be assert on
 * 
 * ----->  Test command : sbt test or sbt testOnly topNtest
 * 
 * 
 */

class topNtest  extends FunSuite 
                with SharedSparkContext
                with DataFrameSuiteBase{
  
  override implicit def reuseContextIfPossible: Boolean = true
  
  test("Counts in Sample file"){
    val sqlContext = new SQLContext(sc)
    val topNObi = new topn()
    val rawData = sc.textFile("./src/test/resources/sample.txt")
    val dataDF = rawData.map(topNObi.parse_apache_log_line).cache()
    val access_logs =   sqlContext.createDataFrame(dataDF.filter(!_.getString(0).contains("Not Matching")), topNObi.getSchema())
    topNObi.calculateTopn(access_logs, sqlContext).registerTempTable("topn")
    val test_val = sqlContext.sql("select freq from topn where ip_address == '199.72.81.55'").first()
    val test_abc = sqlContext.sql("select freq from topn where ip_address == 'abc.com'").first()
    val test_nbc = sqlContext.sql("select freq from topn where ip_address == 'nbc.com'").first()
    
    assert(test_val.get(0) == 15)
    assert(test_abc.get(0) == 4)
    assert(test_nbc.get(0) == 1)
 }
}