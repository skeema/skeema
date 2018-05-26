CREATE TABLE `activity` (
  `user_id` bigint(20) unsigned NOT NULL,
  `action_id` int(10) unsigned NOT NULL,
  `ts` int(10) unsigned NOT NULL,
  `target_type` varchar(20) DEFAULT NULL,
  `target_id` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`user_id`,`action_id`,`ts`),
  KEY `by_target` (`target_id`,`target_type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
