CREATE TABLE `kafka_store` (
  `topic` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `partition` int(11) NOT NULL,
  `startOffset` bigint(20) NOT NULL,
  `finalOffset` bigint(20) NOT NULL,
  `byteSize` bigint(20) NOT NULL,
  `md5Hash` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `time` datetime NOT NULL,
  PRIMARY KEY (`topic`,`partition`,`startOffset`),
  KEY `time_index` (`topic`,`partition`,`time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
