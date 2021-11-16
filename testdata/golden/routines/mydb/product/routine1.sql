DELIMITER //
CREATE DEFINER=`root`@`%` FUNCTION `routine1`(a int,
  b int) RETURNS int(11)
    DETERMINISTIC
BEGIN
	return a * b;
END//
DELIMITER ;
