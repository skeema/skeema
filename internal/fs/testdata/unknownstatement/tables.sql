CREATE TABLE one (
	id int unsigned NOT NULL,
	name varchar(100) default 'unknown',
	PRIMARY KEY (id)
);

VALUES
ROW(1, "Barclay"),
ROW(5, "Barry"),
ROW(123, "Bert"),
ROW(789, "Bort");

CREATE TABLE `two` ( /* this is another table */
	`id` int unsigned NOT NULL,
	`name` varchar(100) default 'unknown',
	PRIMARY KEY (`id`)
);

