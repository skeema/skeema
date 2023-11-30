CREATE DATABASE product;
USE product

CREATE TABLE `users` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL,
  `credits` decimal(9,2) DEFAULT '10.00',
  `last_modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `posts` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) unsigned NOT NULL,
  `body` text,
  `created_at` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP*/,
  `edited_at` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP*/,
  PRIMARY KEY (`id`),
  KEY `user_created` (`user_id`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `comments` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `post_id` bigint(20) unsigned NOT NULL,
  `user_id` bigint(20) unsigned NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `body` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `subscriptions` (
  `user_id` bigint(20) unsigned NOT NULL,
  `post_id` bigint(20) unsigned NOT NULL,
  `subscribed_at` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`post_id`,`user_id`),
  KEY `user_post` (`user_id`,`post_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


CREATE DATABASE analytics;
USE analytics

CREATE TABLE `activity` (
  `user_id` bigint(20) unsigned NOT NULL,
  `action_id` int(10) unsigned NOT NULL,
  `ts` int(10) unsigned NOT NULL,
  `target_type` varchar(20) DEFAULT NULL,
  `target_id` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`user_id`,`action_id`,`ts`),
  KEY `by_target` (`target_id`,`target_type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `rollups` (
  `metric_id` int(10) unsigned NOT NULL,
  `value` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`metric_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `pageviews` (
  `url` varchar(200) NOT NULL,
  `start_ts` int(10) unsigned NOT NULL,
  `end_ts` int(10) unsigned NOT NULL,
  `views` bigint(20) unsigned DEFAULT NULL,
  `domain` varchar(40) NOT NULL,
  PRIMARY KEY (`url`,`start_ts`,`end_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
