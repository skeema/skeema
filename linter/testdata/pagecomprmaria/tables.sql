# Tables testing behavior of InnoDB page compression clauses in MariaDB 10.2+
CREATE TABLE page_comp_1 (
	actor_id smallint(5) unsigned NOT NULL,
	film_name varchar(60) NOT NULL,
	PRIMARY KEY (actor_id,film_name),
	KEY film_name (film_name)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 PAGE_COMPRESSED=1 PAGE_COMPRESSION_LEVEL=9;

CREATE TABLE page_comp_on (
	actor_id smallint(5) unsigned NOT NULL,
	film_name varchar(60) NOT NULL,
	PRIMARY KEY (actor_id,film_name),
	KEY film_name (film_name)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 PAGE_COMPRESSED='on';

CREATE TABLE page_comp_0 (
	actor_id smallint(5) unsigned NOT NULL,
	film_name varchar(60) NOT NULL,
	PRIMARY KEY (actor_id,film_name),
	KEY film_name (film_name)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 PAGE_COMPRESSED=0;

CREATE TABLE page_comp_off (
	actor_id smallint(5) unsigned NOT NULL,
	film_name varchar(60) NOT NULL,
	PRIMARY KEY (actor_id,film_name),
	KEY film_name (film_name)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 PAGE_COMPRESSED='Off';
