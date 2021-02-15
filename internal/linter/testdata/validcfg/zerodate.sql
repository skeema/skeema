CREATE TABLE `zerodate` (
  id int(10) unsigned NOT NULL,
  time_is_fine time, /* annotations: has-time */
  date_zero_year_is_bad date DEFAULT '0000-01-01', /* annotations: zero-date */
  datetime_zero_day_is_bad datetime DEFAULT '2020-01-00 00:00:00', /* annotations: zero-date, has-time */
  timestamp_is_fine timestamp, /* annotations: has-time */
  timestamp_nullable_is_fine timestamp NULL DEFAULT NULL, /* annotations: has-time */
  timestamp_zero_date_is_bad timestamp DEFAULT '0000-00-00 00:00:00', /* annotations: zero-date, has-time */
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;