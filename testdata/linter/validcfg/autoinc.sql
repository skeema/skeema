CREATE TABLE `okautoinc` (
  `id` int(10) unsigned NOT NULL auto_increment,
  `name` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `signedautoinc` (
  `id` int(11) NOT NULL auto_increment, /* annotations: auto-inc */
  `name` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `smallautoinc` (
  `id` smallint(5) unsigned NOT NULL auto_increment, /* annotations: auto-inc */
  `name` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `exhaustedautoinc` (
  `id` int(10) unsigned NOT NULL auto_increment, /* annotations: auto-inc */
  `name` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4000000000 DEFAULT CHARSET=utf8mb4;

