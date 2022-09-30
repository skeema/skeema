# Intentionally no annotation comments below, since this test validates the
# functionality of the default fast-path for lint-definer, which is permissive
# of all DEFINER values.

DELIMITER //

CREATE DEFINER=`root`@`127.0.0.1` FUNCTION `func1`(a int, b int) RETURNS int(11)
    DETERMINISTIC
BEGIN
	return a * b;
END//

CREATE DEFINER=`nobody`@`localhost` PROCEDURE `proc1`(a int, b int)
    DETERMINISTIC
BEGIN
	INSERT INTO foo(mult) VALUES (a * b);
END//

DELIMITER ;
