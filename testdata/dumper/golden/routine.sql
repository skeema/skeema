DELIMITER //
CREATE DEFINER=`root`@`localhost` FUNCTION `routine1`(a int, b int) RETURNS int(11)
    DETERMINISTIC
BEGIN
	# mid-body comment
	return a * b;
end//
delimiter ;
