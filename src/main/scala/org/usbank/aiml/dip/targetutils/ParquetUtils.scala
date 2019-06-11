package org.usbank.aiml.dip.targetutils

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object ParquetUtils {
  /*method to write into Parquet format by data frame*/
  def writeToParquet(df: DataFrame,tgtFile: String): Unit ={
    df.write.mode("append")parquet(tgtFile)
  }

  def readFromParquet(sqlContext: SparkSession, tgtFile: String): DataFrame ={
    val newDataDF = sqlContext.read.parquet(tgtFile)
    return newDataDF
  }



}