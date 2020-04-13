package com.hugh.utils

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

/**
 * @Author Fly.Hugh
 * @Date 2020/3/23 17:54
 * @Version 1.0
 **/
object randomUtils {

  // randomly product int from a to b [a,b]
  def rangeRandomInt(a:Int,b:Int):Int = {
    val c: Double = Math.random() * b + a
    c.toInt
  }

  // generate random number by the length of n
  def generateRandomNumber(n: Int): Long = {
    if (n < 1)
      throw new IllegalArgumentException("随机数位数必须大于0")
    (Math.random * 9 * Math.pow(10, n - 1)).toLong + Math.pow(10, n - 1).toLong
  }

  // generate randomly timestamp in year n by length of 15
  def randomYearTimestamp(n: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy")
    val dateFrom: Date = dateFormat.parse(n.toString)
    val timeStampFrom: Long = dateFrom.getTime
    val dateTo: Date = dateFormat.parse((n + 1).toString)
    val timeStampTo: Long = dateTo.getTime
    val random = new java.util.Random()
    val timeRange: Long = timeStampTo - timeStampFrom
    val randomTimeStamp = timeStampFrom + (random.nextDouble() * timeRange).toLong
    randomTimeStamp.toString
  }

  // randomly generate double d in range (min,max).
  def randomDoubleRange(min: Double, max: Double): Double = {
    val num: Double = (Math.random() * (max - min) + min)
    num
  }

  // randomly generate double d in a Variable range, and the number producted bu this function has a Gaussian distribution.
  def randomDoubleGaussian(c:Int,f:Int):Double = {
    // c: center of the range.   f: size of the range
    val num: Double = Random.nextGaussian() * f + c
    num
  }

  // Keep some(r) digits behind the decimal point
  def getLimitLengthDouble(d:Double,r:Int):Double= {
    BigDecimal(d).setScale(r,BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}