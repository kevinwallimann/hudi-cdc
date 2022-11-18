package org.github.kevinwallimann

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Date

class MySparkTest extends AnyFlatSpec with SparkTestBase {
  behavior of "Spark"

  it should "upsert the rows" in {
    def executeQuery(df: DataFrame) = {
      import org.apache.spark.sql.functions._
      val path = "/tmp/hudi-cdc-3"
      val query = HudiIngestor.upsertStream(df, Seq("info_date"), path, "key", "key", Some(col("A_ENTTYP") === lit("DL")))
      query.processAllAvailable()
    }

    val data1 = TestdataGenerator.insertOnlyData(100, Date.valueOf("2022-07-26"), Date.valueOf("2022-07-30"))
    val schema = data1._1
    val input = MemoryStream[Row](42, spark.sqlContext)(RowEncoder(schema))
    input.addData(data1._2)
    executeQuery(input.toDF())

    val data1Fix = TestdataGenerator.insertOnlyData(100, Date.valueOf("2022-07-26"), Date.valueOf("2022-08-02"), i => if (i == 42) "The Master Hero" else s"Hero_#$i")
    input.addData(data1Fix._2)
    executeQuery(input.toDF())

    val data1Delete = TestdataGenerator.deleteData(Seq(1, 2, 3), Date.valueOf("2022-07-26")) // only works when partition corresponds to key. So in reality, the partition is part of the key for the deletion.
    input.addData(data1Delete._2)
    executeQuery(input.toDF())
    // scala> val df = spark.read.format("hudi").load("/tmp/hudi-cdc")
    // scala> df.where("key==42").show
    // +---+---------------+----------+
    // |key|          value| info_date|
    // +---+---------------+----------+
    // | 42|The Master Hero|2022-07-26|
    // +---+---------------+----------+
    // scala> df.count
    // res0: Long = 697
    // scala> val df0 = spark.read.format("hudi").option("versionAsOf","0").load("/tmp/hudi-cdc")
    // scala> df0.where("key==42").show
    // +---+--------+----------+
    // |key|   value| info_date|
    // +---+--------+----------+
    // | 42|Hero_#42|2022-07-26|
    // +---+--------+----------+
  }
}