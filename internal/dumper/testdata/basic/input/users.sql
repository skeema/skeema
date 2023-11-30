create table users (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL,
  `is_admin` tinyint(1) unsigned,
  `credits` decimal(9,2),
  primary key (`id`),
  unique key `name` (`name`)
) engine=innodb default charset=latin1 collate=latin1_swedish_ci;
