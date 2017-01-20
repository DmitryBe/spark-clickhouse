
import org.scalatest._
import io.clickhouse.ext.ClickhouseConnectionFactory
import io.clickhouse.ext.spark.ClickhouseSparkExt._
import org.apache.spark.sql.SparkSession

case class Row1(name: String, v: Int, v2: Int)

class TestSpec extends FlatSpec with Matchers {
  "case1" should "ok" in {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("local spark")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    // test dframe
    val df = sqlContext.createDataFrame(1 to 10 map(i => Row1(s"$i", i, i + 10)) )

    // clickhouse params
    val anyHost = "localhost"
    val db = "tmp1"
    val tableName = "t1"
//    val clusterName = None: Option[String]
    // start clickhouse docker using config.xml from clickhouse_files
    val clusterName = Some("perftest_1shards_1replicas"): Option[String]

    // define clickhouse connection
    implicit val clickhouseDataSource = ClickhouseConnectionFactory.get(anyHost)

    // create db / table
    df.dropClickhouseDb(db, clusterName)
    df.createClickhouseDb(db, clusterName)
    df.createClickhouseTable(db, tableName, "mock_date", Seq("name"), clusterNameO = clusterName)

    // save data
    val res = df.saveToClickhouse("tmp1", "t1", (row) => java.sql.Date.valueOf("2000-12-01"), "mock_date", clusterNameO = clusterName)
    assert(res.size == 1)
    assert(res.get("localhost") == Some(df.count()))

    true should === (true)
  }
}
