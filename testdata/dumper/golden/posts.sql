/* pre comment preserved   */
CREATE TABLE `posts` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) unsigned NOT NULL,
  `body` varchar(150) DEFAULT NULL,
  `created_at` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_created` (`user_id`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- post comment preserved