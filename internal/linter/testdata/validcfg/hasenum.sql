CREATE TABLE `hasenum` (
  id int(10) unsigned NOT NULL,
  enum_is_bad enum('t1', 't2') , /* annotations: has-enum */
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;