
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import io.clickhouse.ext.ClickhouseConnectionFactory
import io.clickhouse.ext.spark.ClickhouseSparkExt._

case class C1(name: String, v: Int, v2: Int)

class TestSpec extends FlatSpec with Matchers {
  "Hello" should "have tests" in {

    val conf = new SparkConf()
      .setAppName("app")
      .setMaster("local")  // local mode

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.createDataFrame(1 to 10 map(i => C1(s"$i", i, i + 10)) )

    val db = "tmp1"
    val tableName = "t1"

    implicit val conn = ClickhouseConnectionFactory.get("localhost")
    df.dropClickhouseDb(db)
    df.createClickhouseDb(db)
    df.createClickhouseTable(db, tableName, "mock_date", Seq("name"))

    // save data
    df.saveToClickhouse("tmp1", "t1", (row) => java.sql.Date.valueOf("2000-12-01"), "mock_date")

    true should === (true)
  }
}
