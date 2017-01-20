clickhouse spark connector
==========================

> connector #spark DataFrame -> Yandex #ClickHouse table
 
Example
``` scala

    import io.clickhouse.ext.ClickhouseConnectionFactory
    import io.clickhouse.ext.spark.ClickhouseSparkExt._
    import org.apache.spark.sql.SparkSession

    // spark config
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("local spark")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    
    // create test DF
    case class Row1(name: String, v: Int, v2: Int)
    val df = sqlContext.createDataFrame(1 to 1000 map(i => Row1(s"$i", i, i + 10)) )

    // clickhouse params
    
    // any node 
    val anyHost = "localhost"
    val db = "tmp1"
    val tableName = "t1"
    // cluster configuration must be defined in config.xml (clickhouse config)
    val clusterName = Some("perftest_1shards_1replicas"): Option[String]

    // define clickhouse datasource
    implicit val clickhouseDataSource = ClickhouseConnectionFactory.get(anyHost)
    
    // create db / table
    //df.dropClickhouseDb(db, clusterName)
    df.createClickhouseDb(db, clusterName)
    df.createClickhouseTable(db, tableName, "mock_date", Seq("name"), clusterNameO = clusterName)

    // save DF to clickhouse table
    val res = df.saveToClickhouse("tmp1", "t1", (row) => java.sql.Date.valueOf("2000-12-01"), "mock_date", clusterNameO = clusterName)
    assert(res.size == 1)
    assert(res.get("localhost") == Some(df.count()))

```

Docker image
[Docker](https://hub.docker.com/r/dmitryb/clickhouse-spark-connector/)