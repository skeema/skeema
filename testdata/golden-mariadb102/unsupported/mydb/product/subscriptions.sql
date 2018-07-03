CREATE TABLE `subscriptions` (
  `subscription_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) unsigned NOT NULL,
  `post_id` bigint(20) unsigned NOT NULL,
  `subscribed_at` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`post_id`,`user_id`),
  KEY `user_post` (`user_id`,`post_id`),
  KEY `sub_id_user` (`subscription_id`,`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
 PARTITION BY RANGE (`user_id`)
(PARTITION `p0` VALUES LESS THAN (123) ENGINE = InnoDB,
 PARTITION `p1` VALUES LESS THAN MAXVALUE ENGINE = InnoDB);
