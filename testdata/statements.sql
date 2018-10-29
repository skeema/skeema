  -- this file exists for testing statement tokenization of *.sql files

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `product` /*!40100 DEFAULT CHARACTER SET latin1 */;
/* hello */   USE product

CREATE #fun interruption
TABLE `users` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `na``me` varchar(30) NOT NULL DEFAULT 'it\'s complicated "escapes''',
  `credits` decimal(9,2) DEFAULT '10.00', -- end of line; " comment
  `last_modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, # another end-of-line comment;
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
          CREATE TABLE `posts with spaces` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) unsigned NOT NULL,
  `body` varchar(50) DEFAULT '/* lol\'',
  `created_at` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP*/,
  `edited_at` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP*/,
  PRIMARY KEY (`id`),
  KEY `user_created` (`user_id`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

use /*wtf*/`analytics`;CREATE TABLE  if  NOT    eXiStS     `comments` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `post_id` bigint(20) unsigned NOT NULL,
  `user_id` bigint(20) unsigned NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `body` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



CREATE TABLE subscriptions (id int unsigned not null primary key)