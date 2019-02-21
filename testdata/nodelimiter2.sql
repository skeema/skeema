# This should NOT parse "correctly" as a single proc without using the DELIMITER
# command, due to the table after
CREATE PROCEDURE whatever(name varchar(10))
BEGIN
DECLARE v1 INT;
SET v1=loops;
WHILE v1 > 0 DO
INSERT INTO users (name) values ('\xF0\x9D\x8C\x86');
SET v1 = v1 - (2 / 2);
END WHILE;
END;
CREATE TABLE whatever(id int unsigned NOT NULL PRIMARY KEY);
