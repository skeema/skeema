# Coverage for two new features in MariaDB 10.8: DESC index parts, and functions
# with IN / OUT / INOUT param qualifiers

SET foreign_key_checks=0;
SET sql_log_bin=0;

use testing

CREATE TABLE maria108idx (
    a int NOT NULL,
    b int,
    c int,
    d int,
    PRIMARY KEY (a),
    INDEX idx1 (d, b DESC),
    INDEX idx2 (a DESC, b ASC, d DESC)
);

# This is essentially tengo_test.go's aProc() changed to a func instead
delimiter //
CREATE FUNCTION maria108func(
    IN name varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    INOUT iterations int(10) unsigned,   OUT pct decimal(5, 2)
) RETURNS int READS SQL DATA   SQL SECURITY  INVOKER
  BEGIN
  SELECT @iterations + 1, 98.76 INTO iterations, pct;
  RETURN 123;
  END //
delimiter ;

