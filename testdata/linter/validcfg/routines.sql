DELIMITER //

CREATE DEFINER=`root`@`127.0.0.1` FUNCTION `func1`(a int, b int) RETURNS int(11) /* annotations: has-routine */
    DETERMINISTIC
BEGIN
	return a * b;
END//

CREATE DEFINER=`nobody`@`localhost` PROCEDURE `proc1`(a int, b int) /* annotations: has-routine, definer */
    DETERMINISTIC
BEGIN
	INSERT INTO foo(mult) VALUES (a * b);
END//

DELIMITER ;
