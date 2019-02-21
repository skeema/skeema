# This should successfully parse, despite containing a multi-line proc
# without using the DELIMITER command 
CREATE PROCEDURE whatever(name varchar(10))
BEGIN
	DECLARE v1 INT;
	SET v1=loops;
	WHILE v1 > 0 DO
		INSERT INTO users (name) values ('\xF0\x9D\x8C\x86');
		SET v1 = v1 - (2 / 2); /* testing // testing */
	END WHILE;
END;


-- another comment

