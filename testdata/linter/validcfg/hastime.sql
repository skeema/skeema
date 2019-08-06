CREATE TABLE `hastime` (
  id int(10) unsigned NOT NULL,
  time_is_bad time, /*  annotations: has-time */
  date_is_fine date,
  timestamp_is_bad timestamp, /*  annotations: has-time */
  datetime_is_bad datetime, /*  annotations: has-time */
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;