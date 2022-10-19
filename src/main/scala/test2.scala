import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.util._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.FileNotFoundException
import java.io.IOException
import com.typesafe.config.{Config, ConfigFactory}


import org.apache.spark.sql._
object s3read{
  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder().master("local").appName("ReadWrites3").getOrCreate()
    val applicationConf: Config = ConfigFactory.load("application.conf")

    //reading input and output path from application.config file
    val inputPath = applicationConf.getString("app.inputPath")
    val outputPath=applicationConf.getString("app.outputPath")

    // it is possible also to specify keys this way
    val credentialsProviderChain = new DefaultAWSCredentialsProviderChain
    spark.sparkContext.hadoopConfiguration.set("fs.s3.impl", classOf[org.apache.hadoop.fs.s3a.S3AFileSystem].getName)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentialsProviderChain.getCredentials.getAWSAccessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentialsProviderChain.getCredentials.getAWSSecretKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    //Finding the count groupby key
    val result = df.select("key", "value").groupBy("key", "value").count().sort("key", "value")

    //Finding count of odd times group by key
    val r1 = result.select("key", "value").where(col("count") % 2 =!= 0)

    //filtering the key and value having more than one values occur odd number of times
    val r2 = r1.select("key", "value").groupBy("key").count().filter(col("count") =!= 1)

    //creating a list with count
    val ret = r2.select("count").collect()

    //checking the file satisfies the condition
    if (ret.isEmpty) {

      //writing the data to S3
      r1.write.option("header", "true").mode("overwrite").format("csv").option("sep", "\t").save(outputPath)
    }
    else {
      //Throwing error if more than one values occur odd number of times
      throw new Exception("more than one values occur odd number of times")
    }


  }
}