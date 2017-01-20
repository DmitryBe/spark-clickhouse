package io.clickhouse.ext

case class ClusterResultSet(clusterRs: Seq[(String, java.sql.ResultSet)]){
  import io.clickhouse.ext.ClickhouseResultSetExt._

  def get = clusterRs

  def toTab = {
    val firstRow = clusterRs.head
    val firstRowRs = firstRow._2

    val metaTab = if(firstRowRs != null){
      val meta = firstRowRs.getMeta
      ("host" :: meta.map(x => s"${x._2}").toList).mkString("\t")
    }else{
      Seq("host", "result").mkString("\t")
    }

    val bodyTab = clusterRs.map{ cur =>
      val hostIp = cur._1
      if(cur._2 != null){
        val ds = cur._2.getData // Seq[Seq[AnyRef]]
        ds.map{ row =>
          (hostIp :: row.map(v => s"$v").toList).mkString("\t")
        }.mkString("\n")
      }else{
        Seq(hostIp, "null").mkString("\t")
      }
    }.mkString("\n")

    val table = List(metaTab, bodyTab).mkString("\n")
    println(s"%table $table")
  }
}