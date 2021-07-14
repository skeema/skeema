CREATE TABLE nopk ( /* annotations:pk */
	id int unsigned NOT NULL,
	name varchar(30),
  UNIQUE KEY name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
