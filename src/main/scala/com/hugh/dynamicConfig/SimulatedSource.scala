package com.hugh.dynamicConfig

import org.apache.flink.streaming.api.functions.source.SourceFunction
import com.hugh.caseclass.dynamicMessage
import com.hugh.utils.randomUtils.{generateRandomNumber, getLimitLengthDouble, randomDoubleGaussian, randomDoubleRange, randomYearTimestamp, rangeRandomInt}

/**
 * @Author Fly.Hugh
 * @Date 2020/4/7 17:04
 * @Version 1.0
 **/
class SimulatedSource extends SourceFunction[dynamicMessage]{

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
