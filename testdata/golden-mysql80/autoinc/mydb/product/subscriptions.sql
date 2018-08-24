CREATE TABLE `subscriptions` (
  `user_id` bigint(20) unsigned NOT NULL,
  `post_id` bigint(20) unsigned NOT NULL,
  `subscribed_at` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`post_id`,`user_id`),
  KEY `user_post` (`user_id`,`post_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
