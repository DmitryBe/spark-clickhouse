package io.clickhouse.ext

object ClickhouseResultSetExt{
  implicit class ResultSetExt(rs: java.sql.ResultSet){

    def map[T](delegate: (java.sql.ResultSet) => T): Seq[T] = {
      var results = List[T]()
      while(rs.next()){
        results = delegate(rs) :: results
      }
      results
    }

    def toTab = {
      // rs meta: (colId, name, type)
      val header = getMeta.map(v => s"${v._2}").mkString("\t")

      val body = getData.map{ row =>
        row.map(v => s"$v").mkString("\t")
      }.mkString("\n")

      val table = List(header, body).mkString("\n")
      println(s"%table $table")
    }

    def getMeta = {
      1 to rs.getMetaData.getColumnCount map { i =>
        (i, rs.getMetaData.getColumnName(i), rs.getMetaData.getColumnTypeName(i))
      }
    }

    def getData = {
      val meta = getMeta
      val results = scala.collection.mutable.MutableList[Seq[AnyRef]]()
      while(rs.next()){
        val row = meta.map(i => rs.getObject(i._1))
        results += row
      }
      results.toList
    }
  }
}
