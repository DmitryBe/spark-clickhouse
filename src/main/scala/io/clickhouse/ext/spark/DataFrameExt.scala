package io.clickhouse.ext.spark

import io.clickhouse.ext.{ClickhouseClient, ClickhouseConnectionFactory}
import ru.yandex.clickhouse.ClickHouseDataSource
import io.clickhouse.ext.Utils._
import org.apache.spark.sql.types._

object ClickhouseSparkExt{
  implicit def extraOperations(df: org.apache.spark.sql.DataFrame) = DataFrameExt(df)
}

case class DataFrameExt(df: org.apache.spark.sql.DataFrame) extends Serializable {

  def dropClickhouseDb(dbName: String, clusterNameO: Option[String] = None)
                      (implicit ds: ClickHouseDataSource){
    val client = ClickhouseClient(clusterNameO)(ds)
    clusterNameO match {
      case None => client.dropDb(dbName)
      case Some(x) => client.dropDbCluster(dbName)
    }
  }

  def createClickhouseDb(dbName: String, clusterNameO: Option[String] = None)
                        (implicit ds: ClickHouseDataSource){
    val client = ClickhouseClient(clusterNameO)(ds)
    clusterNameO match {
      case None => client.createDb(dbName)
      case Some(x) => client.createDb(dbName)
    }
  }

  def createClickhouseTable(dbName: String, tableName: String, partitionColumnName: String, indexColumns: Seq[String], clusterNameO: Option[String] = None)
                           (implicit ds: ClickHouseDataSource){
    val client = ClickhouseClient(clusterNameO)(ds)
    val sqlStmt = createClickhouseTableDefinitionSQL(dbName, tableName, partitionColumnName, indexColumns)
    clusterNameO match {
      case None => client.query(sqlStmt)
      case Some(x) => client.queryCluster(sqlStmt)
    }
  }

  def saveToClickhouse(dbName: String, tableName: String, partitionFunc: (org.apache.spark.sql.Row) => java.sql.Date, partitionColumnName: String = "mock_date")
                      (implicit ds: ClickHouseDataSource)={
    val host = ds.getHost
    val port = ds.getPort
    val schema = df.schema

    // following code is going to be run on executors
    val insertResults = df.rdd.mapPartitions((partition: Iterator[org.apache.spark.sql.Row])=>{

      val nodeDs = ClickhouseConnectionFactory.get(host, port)

      // explicit closing
      using(nodeDs.getConnection) { conn =>
        val insertStatementSql = generateInsertStatment(schema, dbName, tableName, partitionColumnName)
        val statement = conn.prepareStatement(insertStatementSql)

        partition.foreach{ row =>
          // create mock date
          val partitionVal = partitionFunc(row)
          statement.setDate(1, partitionVal)
          // map fields
          schema.foreach{ f =>
            val fieldName = f.name
            val fieldIdx = row.fieldIndex(fieldName)
            val fieldVal = row.get(fieldIdx)
            if(fieldVal != null)
              statement.setObject(fieldIdx + 2, fieldVal)
            else{
              val defVal = defaultNullValue(f.dataType, fieldVal)
              statement.setObject(fieldIdx + 2, defVal)
            }
          }

          statement.addBatch()
        }

        val r = statement.executeBatch()

        // return: Seq((host, insertCount))
        List(r.sum).toIterator
      }

    }).collect()

    insertResults.sum
  }

  private def generateInsertStatment(schema: org.apache.spark.sql.types.StructType, dbName: String, tableName: String, partitionColumnName: String) = {
    val columns = partitionColumnName :: schema.map(f => f.name).toList
    val vals = 1 to (columns.length) map (i => "?")
    s"INSERT INTO $dbName.$tableName (${columns.mkString(",")}) VALUES (${vals.mkString(",")})"
  }

  private def defaultNullValue(sparkType: org.apache.spark.sql.types.DataType, v: Any) = sparkType match {
    case DoubleType => 0
    case FloatType => 0
    case IntegerType => 0
    case StringType => null
    case BooleanType => false
    case _ => null
  }

  private def createClickhouseTableDefinitionSQL(dbName: String, tableName: String, partitionColumnName: String, indexColumns: Seq[String])= {

    val header = s"""
          CREATE TABLE IF NOT EXISTS $dbName.$tableName(
          """

    val columns = s"$partitionColumnName Date" :: df.schema.map{ f =>
      Seq(f.name, sparkType2ClickhouseType(f.dataType)).mkString(" ")
    }.toList
    val columnsStr = columns.mkString(",\n")

    val footer = s"""
          )ENGINE = MergeTree($partitionColumnName, (${indexColumns.mkString(",")}), 8192);
          """

    Seq(header, columnsStr, footer).mkString("\n")
  }

  private def sparkType2ClickhouseType(sparkType: org.apache.spark.sql.types.DataType)= sparkType match {
    case DoubleType => "Float64"
    case FloatType => "Float32"
    case IntegerType => "Int32"
    case StringType => "String"
    case BooleanType => "UInt8"
    case _ => "unknown"
  }

}
