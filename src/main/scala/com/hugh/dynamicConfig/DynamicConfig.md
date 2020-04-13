# 通过MySQL动态配置CEP规则
## MySQL表结构
根据报警需求，SQL表的结构较为简单，仅仅需要 部门id+阈值就可以配置

后续可能有进一步的需求，会改用Flink SQL编写，这里仅仅讨论部门 id + 阈值即可。

kafka流消息格式
```scala
case class dynamicMessage(
                          imei:String,
                          id: String,   // id of project
                          lat: Double,
                          lng: Double,
                          time: String,
                          speed: Double
                         )
```

MySQL表结构
id String
阈值 Double
```mysql
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;
-- ----------------------------
-- Table structure for event_mapping
-- ----------------------------
DROP TABLE IF EXISTS `event_mapping`;
CREATE TABLE `event_mapping`  (
    `id` varchar(10) NOT NULL COLLATE utf8_bin COMMENT 'Project ID',
    `threshold` Double NOT NULL COLLATE utf8_bin COMMENT 'threshold of speed',
    PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4 CHARACTER SET = utf8 COLLATE = utf8_bin ROW_FORMAT = Dynamic;
-- ----------------------------
-- Records of event_mapping
-- ----------------------------
INSERT INTO `event_mapping` VALUES ('001',60.0);
SET FOREIGN_KEY_CHECKS = 1;
```


