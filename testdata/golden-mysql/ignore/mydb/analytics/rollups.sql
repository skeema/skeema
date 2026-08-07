CREATE TABLE `rollups` (
  `metric_id` int(10) unsigned NOT NULL,
  `value` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`metric_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
