DELIMITER ;
CREATE TABLE one (
	id int unsigned NOT NULL,
	name varchar(100) default 'unknown',
	PRIMARY KEY (id)
);
DELIMITER ;;
CREATE PROCEDURE whatever(name varchar(10))
BEGIN
	DECLARE v1 INT;
	SET v1=loops;
	WHILE v1 > 0 DO
		INSERT INTO users (name) values ('\xF0\x9D\x8C\x86');
		SET v1 = v1 - (2 / 2); /* testing ;; testing */
	END WHILE;
END;;

DELIMITER $$
DELIMITER $$
CREATE TABLE `two` ( /* this is another table */
	`id` int unsigned NOT NULL,
	`name` varchar(100) default 'unknown',
	PRIMARY KEY (`id`)
)$$

