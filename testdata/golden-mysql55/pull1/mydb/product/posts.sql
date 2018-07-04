CREATE TABLE `posts` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) unsigned NOT NULL,
  `body` text,
  `created_at` datetime DEFAULT NULL,
  `edited_at` datetime DEFAULT NULL,
  `status` varchar(20) DEFAULT 'published',
  PRIMARY KEY (`id`),
  KEY `user_created` (`user_id`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
