package com.hugh.kafkaFlink2mq

import java.nio.charset.StandardCharsets

import com.hugh.java.rocket.common.serialization.KeyValueSerializationSchema

/**
 * @program: FlinkPractice
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-21 18:52
 **/
class SimpleStringSerializationSchema extends  KeyValueSerializationSchema[(String,alarmMessage)]{
  val DEFAULT_KEY_FIELD = "key"
  val DEFAULT_VALUE_FIELD = "value"

  var keyField: String = null
  var valueField: String = null


  /**
   * SimpleKeyValueSerializationSchema Constructor.
   *
   * @param keyField   tuple field for selecting the key
   * @param valueField tuple field for selecting the value
   */
  def this(keyField: String, valueField: String) {
    this()
    this.keyField = keyField
    this.valueField = valueField
  }

/*  override def serializeKey(tuple: (String,String)): Array[Byte] = {

    if (tuple == null || keyField == null) return null
    val key = tuple._1
    if (key != null) key.toString.getBytes(StandardCharsets.UTF_8) else null
  }

  override def serializeValue(tuple: (String,String)): Array[Byte] = {
    if (tuple == null || valueField == null) return null
    val value: String = tuple._2.imei+ " " + tuple._2.speed
    if (value != null) value.getBytes(StandardCharsets.UTF_8) else null
  }*/
  override def serializeKey(tuple: (String,alarmMessage)): Array[Byte] = {
    "alarmMessage: ".getBytes(StandardCharsets.UTF_8)
  }

  override def serializeValue(tuple: (String,alarmMessage)): Array[Byte] = {
    val value = tuple._2.imei + " " + tuple._2.speed
    value.getBytes(StandardCharsets.UTF_8)
  }
}