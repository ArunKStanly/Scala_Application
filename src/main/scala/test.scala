import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.FileNotFoundException
import java.io.IOException



object readS3_writes3 {
  def main(args:Array[String]) {

    val spark = SparkSession.builder().master("local[*]").appName("Read S3 Write s3").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    var path = "s3a://scalad1/Sampledata/s_test2.csv"

    //Setting the credentials of AWS
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAT2WYZTKAQLLTXLPC")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    try{

      //Reading CSV file from S3 location
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)

      //Finding the count groupby key
      val result = df.select("key","value").groupBy("key","value").count().sort("key","value")

      //Finding count of odd times group by key
      val r1 = result.select("key","value").where(col("count") %2 =!= 0)

      //filtering the key and value having more than one values occur odd number of times
      val r2 = r1.select("key", "value").groupBy("key").count().filter(col("count") =!= 1)
      
      //creating a list with count
      val ret= r2.select("count").collect()

      //checking the file satisfies the condition
      if(ret.isEmpty) {

        //writing the data to S3
        r2.write.option("header","true").mode("overwrite").format("csv").option("sep"," ").save("s3a://scalad1/output/")
      }
      else
      {
        //Throwing error if more than one values occur odd number of times
        throw new Exception("more than one values occur odd number of times")
      }
    } catch{
      case ex1: FileNotFoundException => println(s"File Not Found")
      case ex2: IOException => println(s"Input/Output Parameter is Missing")

    }
  }
}