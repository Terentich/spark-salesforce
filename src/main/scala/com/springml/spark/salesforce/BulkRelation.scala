package com.springml.spark.salesforce

import java.io.{StringReader, StringWriter}

import com.springml.salesforce.wave.api.{APIFactory, BulkAPI}
import com.springml.salesforce.wave.model.JobInfo
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings, CsvWriter, CsvWriterSettings}
import org.apache.http.Header
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

/**
  * Relation class for reading data from Salesforce and construct RDD
  */
case class BulkRelation(username: String,
                        password: String,
                        login: String,
                        version: String,
                        query: String,
                        sfObject: String,
                        customHeaders: List[Header],
                        userSchema: StructType,
                        sqlContext: SQLContext,
                        inferSchema: Boolean,
                        timeout: Long) extends BaseRelation with TableScan {

  import sqlContext.sparkSession.implicits._

  def buildScan(): RDD[Row] = records.rdd

  private val bulkAPI = APIFactory.getInstance().bulkAPI(username, password, login, version)

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      records.schema
    }
  }

  lazy val records: DataFrame = {
    val inputJobInfo = new JobInfo("CSV", sfObject, "query")
    val jobInfo = bulkAPI.createJob(inputJobInfo, customHeaders.asJava)
    val jobId = jobInfo.getId

    val batchInfo = bulkAPI.addBatch(jobId, query)

    if (awaitJobCompleted(bulkAPI, jobId)) {
      bulkAPI.closeJob(jobId)

      val batchInfoList = bulkAPI.getBatchInfoList(jobId)
      val batchInfos = batchInfoList.getBatchInfo.asScala.toList
      val completedBatchInfos = batchInfos.filter(batchInfo => batchInfo.getState.equals("Completed"))
      val completedBatchInfoIds = completedBatchInfos.map(batchInfo => batchInfo.getId)

      val fetchBatchInfo = (bulkAPI: BulkAPI, batchInfoId: String) => {
        val resultIds = bulkAPI.getBatchResultIds(jobId, batchInfoId)

        val result = bulkAPI.getBatchResult(jobId, batchInfoId, resultIds.get(resultIds.size() - 1))

        // Use Csv parser to split CSV by rows to cover edge cases (ex. escaped characters, new line within string, etc)
        def splitCsvByRows(csvString: String): Seq[String] = {
          // The CsvParser interface only interacts with IO, so StringReader and StringWriter
          val inputReader = new StringReader(csvString)

          val parserSettings = new CsvParserSettings()
          parserSettings.setLineSeparatorDetectionEnabled(true)
          parserSettings.getFormat.setNormalizedNewline(' ')

          val readerParser = new CsvParser(parserSettings)
          val parsedInput = readerParser.parseAll(inputReader).asScala

          val outputWriter = new StringWriter()

          val writerSettings = new CsvWriterSettings()
          writerSettings.setQuoteAllFields(true)
          writerSettings.setQuoteEscapingEnabled(true)

          val writer = new CsvWriter(outputWriter, writerSettings)
          parsedInput.foreach {
            writer.writeRow
          }

          outputWriter.toString.lines.toList
        }

        splitCsvByRows(result)
      }

      val csvData = sqlContext
        .sparkContext
        .parallelize(completedBatchInfoIds)
        .flatMap(fetchBatchInfo(bulkAPI, _)).toDS()

      sqlContext
        .sparkSession
        .read
        .option("header", value = true)
        .option("inferSchema", inferSchema)
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", value = true)
        .csv(csvData)
    } else {
      bulkAPI.closeJob(jobId)
      throw new Exception("Job completion timeout")
    }
  }

  private def awaitJobCompleted(bulkAPI: BulkAPI, jobId: String): Boolean = {
    val timeoutDuration = FiniteDuration(timeout, MILLISECONDS)
    val initSleepIntervalDuration = FiniteDuration(200L, MILLISECONDS)
    val maxSleepIntervalDuration = FiniteDuration(10000L, MILLISECONDS)
    var completed = false
    Utils.retryWithExponentialBackoff(() => {
      completed = bulkAPI.isCompleted(jobId)
      completed
    }, timeoutDuration, initSleepIntervalDuration, maxSleepIntervalDuration)

    completed
  }
}