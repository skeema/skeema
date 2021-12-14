CREATE TABLE one (
	id int unsigned NOT NULL,
	name varchar(100) default 'unknown',
	PRIMARY KEY (id)
);

INSERT INTO one (id, name) VALUES
(1, "Barclay"),
(5, "Barry"),
(123, "Bert"),
(789, "Bort");

CREATE TABLE `two` ( /* this is another table */
	`id` int unsigned NOT NULL,
	`name` varchar(100) default 'unknown',
	PRIMARY KEY (`id`)
);

