
package org.github.kevinwallimann

import org.apache.hudi.DataSourceWriteOptions.{HIVE_STYLE_PARTITIONING, KEYGENERATOR_CLASS_NAME, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_NAME}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

object HudiIngestor {
  def upsertStream(df: DataFrame, partitionColumns: Seq[String], destination: String, keyColumn: String, timestampColumn: String, deletedExprOpt: Option[Column]): StreamingQuery = {
    val dfWithDeletedCol = deletedExprOpt match {
      case Some(deletedExpr) => df.withColumn("_hoodie_is_deleted", deletedExpr)
      case None => df
    }

    val dsw1 = dfWithDeletedCol.writeStream
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(), timestampColumn)
      .option(RECORDKEY_FIELD.key(), keyColumn)
      .option(KEYGENERATOR_CLASS_NAME.key(), classOf[ComplexKeyGenerator].getName)
      .option(PARTITIONPATH_FIELD.key(), partitionColumns.mkString(","))
      .option(HIVE_STYLE_PARTITIONING.key(), "true")
      .option(TABLE_NAME.key(), destination)
      .option("hoodie.table.name", destination)
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", s"${destination}/_checkpoints")
//    val dsw2 = if (partitionColumns.nonEmpty) {
//      dsw1.partitionBy(partitionColumns: _*)
//    } else {
//      dsw1
//    }
    dsw1.start(destination)
  }
}
