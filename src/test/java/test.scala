import scala.util.Random
import scala.util.parsing.json.{JSONArray, JSONObject}

/**
 * @Author Fly.Hugh
 * @Date 2020/3/23 17:24
 * @Version 1.0
 **/
object test {
  def main(args: Array[String]): Unit = {

    val strings = List("001", "002", "003")

/*      val a: Double = Math.random() * 3 + 1
      println(Math.floor(a).toInt)*/

    while(true) {

      println(com.hugh.utils.randomUtils.rangeRandomInt(1,3))
      Thread.sleep(500)

    }
  }

}
