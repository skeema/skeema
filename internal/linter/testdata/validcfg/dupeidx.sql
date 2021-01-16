CREATE TABLE `dupeidx` (
  `id` int(10) unsigned NOT NULL,
  `name` varchar(30) DEFAULT NULL,
  `one` int,
  `two` int,
  `three` int,
  `four` int,
  `five` int,
  PRIMARY KEY (`id`),
  KEY onetwo (one, two), /* annotations: dupe-index */
  KEY onetwothree (one, two, three),
  KEY idnamefive (id, name, five),
  UNIQUE KEY idname (id, name),
  UNIQUE KEY name (name),
  KEY id (id), /* annotations: dupe-index */
  KEY idnamefivemulti (id, name, five), /* annotations: dupe-index */
  KEY onetwothreeagain (one, two, three) /* annotations: dupe-index */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4