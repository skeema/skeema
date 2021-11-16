DELIMITER //
create definer=`root`@`%` function `routine1`(a int, b int) returns int(11) deterministic
BEGIN
	# mid-body comment
	return a * b;
end//
delimiter ;
