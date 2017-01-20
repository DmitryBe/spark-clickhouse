import java.util.Properties

import org.scalatest.{FlatSpec, Matchers}
import io.clickhouse.ext.Utils._

class UtilsSpec extends FlatSpec with Matchers{

  "case 1" should "ok" in {

    var f = false
    case class Mock(){
      def print(): Unit = {
        println("mock print")
      }
      def close(): Unit ={
        f = true
      }
    }

    using(Mock()){ mock =>
      mock.print()
    }
    assert(f.equals(true))
  }

}
