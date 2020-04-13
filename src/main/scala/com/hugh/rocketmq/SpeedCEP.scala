package com.hugh.rocketmq


import java.text.SimpleDateFormat
import java.util.{Date, List, Properties}

import com.hugh.java.rocket.common.selector.DefaultTopicSelector
import com.hugh.java.rocket.{RocketMQConfig, RocketMQSink}
import com.hugh.kafkaFlink2mq.{SimpleStringSerializationSchema, alarmMessage}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.cep.scala.{CEP, PatternStream}

import scala.util.Random
import scala.collection.Map

/**
 * @program: FlinkPractice
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-19 22:07
 **/

case class Message(imei:String,
                   lat:String,
                   lng:String,
                   time:String,
                   speed:Double)

object SpeedCEP {

  final val MAX_SPEED = 120
  final val MIN_SPEED = 60

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[Message] = env.addSource(new speedSource)

    val messagePattern: Pattern[Message, Message] = Pattern.begin[Message]("begin")
      .subtype(classOf[Message])
      .where(_.speed > MAX_SPEED)
      .or(_.speed < MIN_SPEED)
      .oneOrMore

    val patternStream: PatternStream[Message] = CEP.pattern(stream.keyBy(_.imei),messagePattern)

    /*patternStream.select(
      (pattern:Map[String,Iterable[LoginMessage]]) => {
        val first = pattern.getOrElse("begin",null).iterator.next()
        val second =
      }
    )*/

    val loginMessageStream: DataStream[(String, alarmMessage)] = patternStream.select(
      (pattern: Map[String, Iterable[Message]]) => {
      val first: Message = pattern.getOrElse("begin",null).iterator.next()

        alarmMessage(first.imei,first.speed)

    }).map(
      line =>
        ("alarmMessage",line)
    )

    val producerProps = new Properties
    producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "192.168.229.131:9876")
    val msgDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL05
    producerProps.setProperty(RocketMQConfig.MSG_DELAY_LEVEL, String.valueOf(msgDelayLevel))
    //   TimeDelayLevel is not supported for batching
    val batchFlag = msgDelayLevel <= 0

    val schema = new SimpleStringSerializationSchema

    loginMessageStream.addSink(new RocketMQSink[(String,alarmMessage)](schema,new DefaultTopicSelector[(String,alarmMessage)]("flinksink2mq"),producerProps).withBatchFlushOnCheckpoint(batchFlag))

    env.execute("loginMessageStream")
  }
}

// simulated data of Source
class speedSource extends SourceFunction[Message]{

  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[Message]): Unit = {

    while(running){
      sourceContext.collect(
        Message(generateRandomNumber(15).toString,
          randomLonLat(0,180),randomLonLat(0,180),
          RandomYearTimestamp(2019),randomSpeed(100)))
    }
  }

  override def cancel(): Unit = {
    running = false
  }

  // 生成n位随机数 生成随机15位imei
  def generateRandomNumber(n:Int):Long = {
    if(n<1)
      throw new IllegalArgumentException("随机数位数必须大于0")
    (Math.random * 9 * Math.pow(10, n - 1)).toLong + Math.pow(10, n - 1).toLong
  }

  // 用于生成n年里的十五位随机时间戳
  def RandomYearTimestamp(n:Int):String={
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy")
    val dateFrom: Date = dateFormat.parse(n.toString)
    val timeStampFrom: Long = dateFrom.getTime
    val dateTo: Date = dateFormat.parse((n+1).toString)
    val timeStampTo: Long = dateTo.getTime
    val random = new java.util.Random()
    val timeRange: Long = timeStampTo - timeStampFrom
    val randomTimeStamp = timeStampFrom + (random.nextDouble()*timeRange).toLong
    randomTimeStamp.toString
  }

  /*
    def randomLonLat(MinLon:Double,MaxLon:Double,MinLat:Double,MaxLat:Double):Map[String,String]={
    val db1: BigDecimal = BigDecimal(Math.random() * (MaxLon - MinLon) + MinLon)
    val lon = db1.setScale(6,BigDecimal.RoundingMode.HALF_UP).toString()
    val db2: BigDecimal = BigDecimal(Math.random() * (MaxLat - MinLat) + MinLat)
    val lat = db2.setScale(6,BigDecimal.RoundingMode.HALF_UP).toString()
    val LonLat = Map(lon -> lat)
    LonLat}
    */

  def randomLonLat(Min:Double,Max:Double):String={
    val db: BigDecimal = BigDecimal(Math.random() * (Max - Min) + Min)
    val lonlat = db.setScale(6,BigDecimal.RoundingMode.HALF_UP).toString()
    lonlat
  }

  def randomSpeed(s:Int):Double={
    val speed: Double = Random.nextGaussian() * 10 + s
    val db = BigDecimal(speed)
    val speedDB: BigDecimal = db.setScale(6,BigDecimal.RoundingMode.HALF_UP)
    speedDB.doubleValue()
  }
}
