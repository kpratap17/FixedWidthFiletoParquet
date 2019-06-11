package org.usbank.aiml.dip

import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.usbank.aiml.dip.targetutils.ParquetUtils

object FwfToParquet  {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
  //System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

  /*Method to read fixed width file and write into data frame based on dynamic file metadata*/
  def splitRecByColSize(pos: List[Int], str: String): Row = {
    //System.out.println("Inside the method...")
    val (rest, result) = pos.foldLeft((str, List[String]())) {
      case ((s, res),curr) =>
       // System.out.println("s.length()-->"+ s.length + "--curre--->"+ curr);
        if(s.length()<=curr)

        {

          val split=s.substring(0).trim()

          val rest=""

          (rest, split :: res)

        }

        else if(s.length()>curr)

        {

          val split=s.substring(0, curr).trim()

          val rest=s.substring(curr)

          (rest, split :: res)

        }

        else

        {

          val split=""

          val rest=""

          (rest, split :: res)

        }

    }
    System.out.println("result -->"+ result);
    System.out.println("rest -->"+ rest);
    Row.fromSeq(result.reverse)

  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark-FixedwidthToParquet")
      //.master("local")
      .master("spark://10.168.0.5:7077")
      .getOrCreate()

    /* val conf:SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")
   val sc:SparkContext = new SparkContext(conf)
   val sqlContext = new SQLContext(sc)*/

    /*GCP connection*/
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    conf.set("spark.driver.extraClassPath", "/spark/config/gcs-connector-hadoop2-latest.jar")
    //conf.set("spark.driver.extraClassPath", "/Users/kinjarapu/Desktop/Learnings/GCP/jars/gcs-connector-hadoop2-latest.jar")
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("google.cloud.auth.service.account.enable", "true")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    conf.set("google.cloud.auth.service.account.json.keyfile", "/spark/config/datasciencesandbox-8b67edc56727.json")
    conf.set("spark.speculation", "false")
    /*Use below for better performance --default is JAVA serialization*/
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("google.cloud.auth.service.account.json.keyfile","/Users/kinjarapu/Desktop/Learnings/Springboot-Spark/GCP-key/datasciencesandbox-8b67edc56727.json")


    /*Parase the file metadata json format and create Data Frame*/
    val parseJson = spark.read
      .option("multiline", true)
      .json(args(0))

    //parseJson.show(false)

    /*Extract column information from data frame*/
    val colInfo = parseJson.select(explode(parseJson("columninformation"))).toDF("columninformation")
      .select("columninformation.order", "columninformation.name", "columninformation.type", "columninformation.length", "columninformation.posfrom", "columninformation.posto", "columninformation.universalrule", "columninformation.policyrule")
    //colInfo.show()

    /*Extract Source Metadata from data frame and create source file from metadata*/
    val srcMetadata = parseJson.select("sourceMetadata.sourceFileName", "sourceMetadata.sourceFileLocation", "sourceMetadata.sourceFileFormat", "sourceMetadata.sourceFileType", "sourceMetadata.isEncrypted", "sourceMetadata.encryptionKeySource")
    val srcFile = srcMetadata.selectExpr("concat(sourceFileLocation, sourceFileName) as srcFile").head().getString(0)
    // println("length --->"+srcFile)

    /*Extract Target Metadata from data frame and create target file from metadata*/
    val tgtmetadata = parseJson.select("targetmetadata.targetFilename", "targetmetadata.targetFileLocation", "targetmetadata.targetFormat")
    val tgtFile = tgtmetadata.selectExpr("concat(targetFileLocation, targetFilename) as tgtFile").head().getString(0)
    //tgtFile.show(false)

    /*Extract schedule information from data frame*/
    val schdInfo = parseJson.select("scheduleInformation.scheculeType", "scheduleInformation.scheduleTime")
    //schdInfo.show()


    /*Read source file dynamically based on metadata*/
    val getSrcdata = spark.sqlContext.read.textFile(srcFile)
    //getSrcdata.show(10,false)

    val sourceFileRDD = spark.sparkContext.textFile(srcFile)


    /*get column list from column info to create dynamic schema*/
    val colNames = colInfo.select("name").sort("order").rdd.map(x => x.getString(0).trim()).collect()
    // colNames.foreach(x=>println("COl Names-->"+x))

    val schema = StructType(colNames.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    print(schema)

    /*get column size from column_info*/
    val colSize = colInfo.select("length").sort("order").rdd.map(x => x.getLong(0)).collect().map(_.toInt).toList
    //colSize.foreach(x=>(println("COl values-->"+x)))

    //sourceFileRDD.map(x => println("Data from RDD -->" + x))

    println("count is--->" + getSrcdata.rdd.count())


    /*Convert fixed width file to dataframe based on dynamic schema*/
    val convertFWFToDF = spark.createDataFrame(sourceFileRDD.map { x => splitRecByColSize(colSize, x) }, schema)
    //convertFWFToDF.filter("RECORDTYPE == 11").show(false)
    //convertFWFToDF.show(10,false)

    /*Write into Parquet file Dynamically*/
    ParquetUtils.writeToParquet(convertFWFToDF,tgtFile)

    /*Read data from Parquet File*/
      val rdf = ParquetUtils.readFromParquet(spark,tgtFile)
    println("Count after reading from Parquet -->" + rdf.show(10,false));

  }

}

