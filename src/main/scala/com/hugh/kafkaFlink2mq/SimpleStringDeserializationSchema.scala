package com.hugh.kafkaFlink2mq

import com.hugh.java.rocket.common.serialization.KeyValueDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
 * @program: FlinkPractice
 * @description: ${description}
 * @author: Fly.Hugh
 * @create: 2020-03-21 18:43
 **/
class SimpleStringDeserializationSchema extends  KeyValueDeserializationSchema[(String,String)]{

  private val serialVersionUID = 1L

  override def deserializeKeyAndValue(key: Array[Byte], value: Array[Byte]): (String,String) = {
    import java.nio.charset.StandardCharsets
    val k = if (key != null) new String(key, StandardCharsets.UTF_8)    else ""

    val v = if (value != null) new String(value, StandardCharsets.UTF_8)
    else ""
    (k,v)
  }

  override def getProducedType: TypeInformation[(String,String)] = {
    return TypeInformation.of(classOf[(String,String)])
  }

}
