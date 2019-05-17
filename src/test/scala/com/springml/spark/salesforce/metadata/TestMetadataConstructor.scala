package com.springml.spark.salesforce.metadata

import com.springml.spark.salesforce.Utils
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

/**
  */
class TestMetadataConstructor extends FunSuite {

  test("Test Metadata generation") {
    val columnNames = List("c1", "c2", "c3", "c4")
    val columnStruct = columnNames.map(colName => StructField(colName, StringType, nullable = true))
    val schema = StructType(columnStruct)

    val schemaString = MetadataConstructor.generateMetaString(schema, "sampleDataSet", Utils.metadataConfig(null))
    assert(schemaString.length > 0)
    assert(schemaString.contains("sampleDataSet"))
  }

  test("Test Metadata generation With Custom MetadataConfig") {
    val intField = StructField("intCol", IntegerType, nullable = true)
    val longField = StructField("longCol", LongType, nullable = true)
    val floatField = StructField("floatCol", FloatType, nullable = true)
    val dateField = StructField("dateCol", DateType, nullable = true)
    val timestampField = StructField("timestampCol", TimestampType, nullable = true)
    val stringField = StructField("stringCol", StringType, nullable = true)
    val someTypeField = StructField("someTypeCol", BooleanType, nullable = true)

    val columnStruct = Array[StructField](intField, longField, floatField, dateField, timestampField, stringField, someTypeField)

    val schema = StructType(columnStruct)

    var metadataConfig = Map("string" -> Map("wave_type" -> "Text"))
    metadataConfig += ("integer" -> Map("wave_type" -> "Numeric", "precision" -> "10", "scale" -> "0", "defaultValue" -> "100"))
    metadataConfig += ("float" -> Map("wave_type" -> "Numeric", "precision" -> "10", "scale" -> "2"))
    metadataConfig += ("long" -> Map("wave_type" -> "Numeric", "precision" -> "18", "scale" -> "0"))
    metadataConfig += ("date" -> Map("wave_type" -> "Date", "format" -> "yyyy/MM/dd"))
    metadataConfig += ("timestamp" -> Map("wave_type" -> "Date", "format" -> "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))


    val schemaString = MetadataConstructor.generateMetaString(schema, "sampleDataSet", metadataConfig)
    assert(schemaString.length > 0)
    assert(schemaString.contains("sampleDataSet"))
    assert(schemaString.contains("Numeric"))
    assert(schemaString.contains("precision"))
    assert(schemaString.contains("scale"))
    assert(schemaString.contains("18"))
    assert(schemaString.contains("Text"))
    assert(schemaString.contains("Date"))
    assert(schemaString.contains("format"))
    assert(schemaString.contains("defaultValue"))
    assert(schemaString.contains("100"))
    assert(schemaString.contains("yyyy/MM/dd"))
    assert(schemaString.contains("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
  }
}