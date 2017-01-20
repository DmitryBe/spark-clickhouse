package io.clickhouse.ext

import java.util.Properties
import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties

object ClickhouseConnectionFactory extends Serializable{

  private val dataSources = scala.collection.mutable.Map[(String, Int), ClickHouseDataSource]()

  def get(host: String, port: Int = 8123): ClickHouseDataSource ={
    dataSources.get((host, port)) match {
      case Some(ds) =>
        ds
      case None =>
        val ds = createDatasource(host, port = port)
        dataSources += ((host, port) -> ds)
        ds
    }
  }

  private def createDatasource(host: String, dbO: Option[String] = None, port: Int = 8123) = {
    val props = new Properties()
    dbO map {db => props.setProperty("database", db)}

    val clickHouseProps = new ClickHouseProperties(props)
    new ClickHouseDataSource(s"jdbc:clickhouse://$host:$port", clickHouseProps)
  }
}
