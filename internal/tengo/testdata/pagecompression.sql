# Table using InnoDB transparent page compression (MySQL 5.7+)
# Keep in sync with integration.sql's actor_in_film
use testing
CREATE TABLE actor_in_film_comp (
	actor_id smallint(5) unsigned NOT NULL,
	film_name varchar(60) NOT NULL,
	PRIMARY KEY (actor_id,film_name),
	KEY film_name (film_name)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMPRESSION='zlib';
