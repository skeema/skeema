# Table using InnoDB transparent page compression with MariaDB formatting.
# Although this feature is supported in MariaDB 10.1, the syntax may differ,
# so only 10.2+ is tested.
# Keep in sync with integration.sql's actor_in_film
use testing
CREATE TABLE actor_in_film_comp (
	actor_id smallint(5) unsigned NOT NULL,
	film_name varchar(60) NOT NULL,
	PRIMARY KEY (actor_id,film_name),
	KEY film_name (film_name)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci PAGE_COMPRESSED=1 PAGE_COMPRESSION_LEVEL=9;
