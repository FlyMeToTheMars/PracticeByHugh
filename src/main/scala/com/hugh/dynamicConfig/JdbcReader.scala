package com.hugh.dynamicConfig

import com.hugh.caseclass.RuleInFo
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration

/**
 * @Author Fly.Hugh
 * @Date 2020/4/7 17:12
 * @Version 1.0
 **/
class JdbcReader extends RichSourceFunction[RuleInFo]{
  private var connection:Connection = null
  private var ps:PreparedStatement = null
  private var isRunning:Boolean = true
  override def cancel(): Unit = {
    super.close()
    if(connection!=null){
      connection.close()
    }
    if(ps != null){
      ps.close()
    }
    isRunning = false
  }
  override def run(ctx: SourceFunction.SourceContext[RuleInFo]): Unit = {
    while (isRunning){
      val resultSet = ps.executeQuery()
      while (resultSet.next()){
        val rangeInfo =RuleInFo(resultSet.getString("id"),resultSet.getDouble("threshold"))
        ctx.collect(rangeInfo)
      }

      Thread.sleep(5000 * 60)
    }
  }
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val driverClass = "com.mysql.jdbc.Driver"
    val dbUrl = "jdbc:mysql://localhost:3306/flink"
    val userName = "root"
    val passWord = "1234"
    connection = DriverManager.getConnection(dbUrl, userName, passWord)
    ps = connection.prepareStatement("select region, value, inner_code from event_mapping")
  }
}
