/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hugh.java.rocket.example;

import org.apache.log4j.Logger;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;


public class SimpleProducer {
    private static Logger logger = Logger.getLogger(SimpleProducer.class);

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("p001");
        producer.setNamesrvAddr("192.168.229.131:9876");
        try {
            producer.start();
            logger.info("producer启动成功");
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 10000; i++) {
            Message msg = new Message("testTopic", "", "id_" + i, ("country_X province_" + i).getBytes());
            try {
                producer.send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("send " + i);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
