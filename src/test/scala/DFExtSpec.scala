
import org.scalatest._
import io.clickhouse.ext.ClickhouseConnectionFactory
import io.clickhouse.ext.spark.ClickhouseSparkExt._
import org.apache.spark.sql.SparkSession

case class Row1(name: String, v: Int, v2: Int)

class TestSpec extends FlatSpec with Matchers {

  "case0" should "" in {

    val max = 25e6
    val monthSize = max / 11
    val daySize = monthSize / 28

    def yearMap(chrom: String) = {
      1900 + (math.abs(chrom.hashCode) % 200)
    }

    def monthDayMap(pos: Int) = {
      val m = (pos / monthSize).toInt
      val d = ((pos % monthSize) / daySize).toInt
      (m + 1, d + 1)
    }

    val r = (5024637 to 48119824).toList map { pos =>
      monthDayMap(pos)
    }

    val month_range = r.map(_._1).distinct
    val day_range = r.map(_._2).distinct

    assert(true)
  }

  "case 11" should "" in {

    val a = 1

    def calc(pos: Int) = {
      val x = pos / 25e6 * 348
      val m = x % 12
      val d = x % 29
      (m.toInt, d.toInt)
    }

    val r = (0 to 1000000).toList map { pos =>
      calc(pos)
    }

    val month_range = r.map(_._1).distinct
    val day_range = r.map(_._2).distinct


    assert(true)
  }

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
