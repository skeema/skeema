DELIMITER ;
CREATE TABLE one (
	id int unsigned NOT NULL,
	name varchar(100) default 'unknown',
	PRIMARY KEY (id)
);

DELIMITER $$
DELIMITER $$
CREATE TABLE `two` ( /* this is another table */
	`id` int unsigned NOT NULL,
	`name` varchar(100) default 'unknown',
	PRIMARY KEY (`id`)
)$$

