DELIMITER //

CREATE DEFINER=`root`@`localhost` FUNCTION `func1`(a int, b int) RETURNS int(11)
    DETERMINISTIC
BEGIN
	return a * b;
END//

CREATE DEFINER=`root`@`localhost` PROCEDURE `proc1`(a int, b int)
    DETERMINISTIC
BEGIN
	INSERT INTO foo(mult) VALUES (a * b);
END//

DELIMITER ;
