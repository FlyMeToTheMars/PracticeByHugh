package com.hugh.sketch

import com.hugh.kafkaFlink2mq.kafkaMessage
import org.apache.flink.cep.scala.pattern.Pattern

/**
 * @Author Fly.Hugh
 * @Date 2020/3/24 10:12
 * @Version 1.0
 **/
object dynamicSpeedUtil {

  final val SPEED_LIMIT_001 = 60
  final val SPEED_LIMIT_002 = 70
  final val SPEED_LIMIT_003 = 80

  def getPattern(partment:String):Pattern[dynamicMessage, dynamicMessage] = {

    var p:Pattern[dynamicMessage, dynamicMessage] = null

    if(partment == "1"){
      patternGet(SPEED_LIMIT_001)
    }
    if(partment == "2"){
      patternGet(SPEED_LIMIT_002)
    }
    if(partment == "3"){
      patternGet(SPEED_LIMIT_003)
    }
    p
  }

  def patternGet(s:Int):Pattern[dynamicMessage, dynamicMessage]={

    Pattern.begin[dynamicMessage]("begin")
      .subtype(classOf[dynamicMessage])
      .where(_.speed > s)
      .oneOrMore
  }



}
