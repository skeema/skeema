CREATE TABLE `displaywidth` (
  `id` int(10) unsigned NOT NULL,
  
  -- confirm the defaults are correct in the checker
  deftinyint tinyint,
  defsmint smallint,
  defmedint mediumint,
  defint int,
  defbigint bigint,
  deftinyintu tinyint unsigned,
  defsmintu smallint unsigned,
  defmedintu mediumint unsigned,
  defintu int unsigned,
  defbigintu bigint unsigned,

  -- annotations expected here
  badtinyint tinyint(2),            /* annotations: display-width */
  `badmedintu` mediumint(9) unsigned, /* annotations: display-width */
  badint    int(100),                  /* annotations: display-width */
  badbigintu bigint(15) unsigned,   /* annotations: display-width */
  
  -- confirm special-cases don't generate annotations
  booly bool,
  alsobool tinyint(1),
  alsoboolu tinyint(1) unsigned,
  padded int(5) zerofill,
  paddedu int(4) unsigned zerofill,
  
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4