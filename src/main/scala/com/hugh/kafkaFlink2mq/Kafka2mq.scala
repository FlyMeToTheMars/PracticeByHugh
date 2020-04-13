package com.hugh.kafkaFlink2mq

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.hugh.java.rocket.common.selector.DefaultTopicSelector
import com.hugh.java.rocket.{RocketMQConfig, RocketMQSink, RocketMQSource}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.collection.Map
import scala.util.parsing.json.JSONArray

/**
 * @program: FlinkPractice
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-19 23:47
 **/

case class kafkaMessage(imei:String,
                         alarm_type:String,
                         lat:Double,
                         lng:Double,
                         create_time:String,
                         device_status:String,
                         mc_type:String,
                         push_time:String,
                         read_status:String,
                         speed:Double,
                         status:String,
                         addr:String,
                         id:String,
                         index_name:String,
                         user_id:String,
                         user_parent_id:String,
                         extendedfield_3:String,
                         extendedfield_4:String,
                         extendedfield_5:String,
                         years:Int,
                         months:Int,
                         days:Int)

case class alarmMessage(imei:String,
                       speed:Double)

object kafka2mq {

  final val MAX_SPEED = 120
  final val MIN_SPEED = 60

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // support kafka
    val topic = ""
    val kafkaProps = new Properties()

    kafkaProps.setProperty("bootstrap.servers", "")
    kafkaProps.setProperty("group.id", "")

    val consumer = new FlinkKafkaConsumer[String](
      topic,
      new SimpleStringSchema(),
      kafkaProps
    )
    consumer.setStartFromEarliest()
    val kafkaSource: DataStream[String] = env.addSource(consumer)

    val value: DataStream[kafkaMessage] = kafkaSource.map(lines => {
      val json = JSON.parseObject(lines)
      val imei = json.getString("imei")
      val alarm_type = json.getString("alarm_type")
      val lat = json.getDouble("lat")
      val lng = json.getDouble("lng")
      val create_time = json.getString("create_time")
      val device_status = json.getString("device_status")
      val mc_type = json.getString("mc_type")
      val push_time = json.getString("push_time")
      val read_status = json.getString("read_status")
      val speed = json.getDouble("speed")
      val status = json.getString("status")
      val addr = json.getString("addr")
      val id = json.getString("id")
      val index_name = json.getString("index_name")
      val user_id = json.getString("user_id")
      val user_parent_id = json.getString("user_parent_id")
      val extendedfield_3 = json.getString("extendedfield_3")
      val extendedfield_4 = json.getString("extendedfield_4")
      val extendedfield_5 = json.getString("extendedfield_5")
      val years = json.getInteger("years")
      val months = json.getInteger("months")
      val days = json.getInteger("days")
      kafkaMessage(imei,alarm_type,lat,lng,create_time,device_status,mc_type,push_time,read_status,speed,status,addr,id,index_name,user_id,user_parent_id,extendedfield_3,extendedfield_4,extendedfield_5,years,months,days)
    })

    val speedPattern: Pattern[kafkaMessage, kafkaMessage] = Pattern.begin[kafkaMessage]("begin")
      .subtype(classOf[kafkaMessage])
      .where(_.speed > MAX_SPEED)
      .or(_.speed < MIN_SPEED)
      .oneOrMore

    val patternStream: PatternStream[kafkaMessage] = CEP.pattern(value.keyBy(_.imei), speedPattern)

    val alarmMessageStream = patternStream.select(
      (pattern: Map[String, Iterable[kafkaMessage]]) => {
        val am = pattern.getOrElse("begin", null).iterator.next()

        alarmMessage(am.imei, am.speed)
      }
    )

    val producerProps = new Properties
    producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "192.168.52.72:9876")
    val msgDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL05
    producerProps.setProperty(RocketMQConfig.MSG_DELAY_LEVEL, String.valueOf(msgDelayLevel))
    //   TimeDelayLevel is not supported for batching
    val batchFlag = msgDelayLevel <= 0

    val schema = new SimpleStringSerializationSchema

//    alarmMessageStream.addSink(new RocketMQSink[alarmMessage](schema,new DefaultTopicSelector[alarmMessage]("flink-sink2"),producerProps).withBatchFlushOnCheckpoint(batchFlag))

    env.execute("start")
  }

  def builderTurboMQSource:RocketMQSource[(String,String)] = {
    val consumerProps = new Properties()
    consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "192.168.52.72:9876")
    consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "c006")
    consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "flink-source2")
    val schema = new SimpleStringDeserializationSchema
    val turboMQConsumer = new RocketMQSource[(String,String)](schema, consumerProps)
    turboMQConsumer
  }
}