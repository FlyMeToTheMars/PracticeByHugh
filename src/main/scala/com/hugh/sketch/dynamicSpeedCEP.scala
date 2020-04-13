package com.hugh.sketch


import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.hugh.kafkaFlink2mq.alarmMessage
import com.hugh.rocketmq.Message
import com.hugh.utils.randomUtils._
import jdk.nashorn.internal.parser.JSONParser
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE
import com.hugh.sketch.dynamicSpeedUtil.getPattern
import org.apache.flink.streaming.api.datastream.DataStreamSink

import scala.collection.Map
import scala.util.Random

/**
 * @Author Fly.Hugh
 * @Date 2020/3/23 16:35
 * @Version 1.0
 **/
object dynamicSpeedCEP {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[dynamicMessage] = env.addSource(new speedSource)

    dataStream.writeAsText("F:\\Hugh's Onenote\\OneDrive\\Project\\PracticeByHugh\\src\\main\\resources\\dataStream",OVERWRITE)

    val value: DataStream[DataStreamSink[alarmMessage]] = dataStream.map(

      line => {
        // Error Code. My Map Fail.
        val patternStream: PatternStream[dynamicMessage] = CEP
        .pattern(dataStream, getPattern(line.speed.toString))

        val alarmMessageStream: DataStream[alarmMessage] = patternStream.select(
          (pattern: Map[String, Iterable[dynamicMessage]]) => {
            val am = pattern.getOrElse("begin", null).iterator.next()
            alarmMessage(am.imei, am.speed)
          }
        )

        alarmMessageStream.print()
      }
    )

    value.print()

    env.execute("dya exe")
  }


  // simulated data of Source.
  class speedSource extends SourceFunction[dynamicMessage] {

    var running: Boolean = true

    override def run(sourceContext: SourceFunction.SourceContext[dynamicMessage]): Unit = {
      while (running) {
        sourceContext.collect(
          dynamicMessage(
            randomImei(),
            randomId(),
            randomLonLat(),
            randomLonLat(),
            randomYearTimestamp(2019),
            randomSpeed(40)
          )
        )
        Thread.sleep(100)
      }
    }

    override def cancel(): Unit = {
      running = false
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

    def randomImei():String = {
      // imei's length is 15
      generateRandomNumber(15).toString
    }

    def randomLonLat(): Double = {
      val num: Double = randomDoubleRange(0,180)
      getLimitLengthDouble(num,6)
    }

    def randomSpeed(s: Int): Double = {
      // Set Gaussian distribution range 30
      val num = randomDoubleGaussian(s,30)
      // abs: absolute terms
      getLimitLengthDouble(num,6).abs
    }

    def  randomId(): String = {
      "00" + rangeRandomInt(1,3)
    }
  }
}

//new case class
case class dynamicMessage(
                          imei:String,
                          id: String,   // id of project
                          lat: Double,
                          lng: Double,
                          time: String,
                          speed: Double
                         )


