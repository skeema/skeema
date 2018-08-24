CREATE TABLE `pageviews` (
  `url` varchar(200) NOT NULL,
  `start_ts` int(10) unsigned NOT NULL,
  `end_ts` int(10) unsigned NOT NULL,
  `views` bigint(20) unsigned DEFAULT NULL,
  `domain` varchar(40) NOT NULL,
  PRIMARY KEY (`url`,`start_ts`,`end_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
