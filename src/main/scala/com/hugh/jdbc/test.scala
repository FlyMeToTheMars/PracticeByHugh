package com.hugh.jdbc

/**
 * @Author Fly.Hugh
 * @Date 2020/4/8 10:26
 * @Version 1.0
 **/
object test {
  def main(args: Array[String]): Unit = {
    println(JdbcHelper.query("select * from event_mapping;"))
  }
}
