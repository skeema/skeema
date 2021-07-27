# These two tables use traditional InnoDB compression.
#
# MariaDB 10.6+ note: these will error due to deprecation of InnoDB compression
# in this flavor.

CREATE TABLE `innoblock` (
  `id` int(10) unsigned NOT NULL,
  `name` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 KEY_BLOCK_SIZE=2 /* annotations: compression */
;

# no annotations on this next one, since by default this will be equivalent
# to KEY_BLOCK_SIZE=8 which is permitted with the default allow-list.
CREATE TABLE `innoblockdefault` (
  `id` int(10) unsigned NOT NULL,
  `name` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED
;