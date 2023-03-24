CREATE TABLE `sqlexception` ( /* annotations:reserved-word */
  id int unsigned NOT NULL primary key,
  `show` int unsigned DEFAULT NULL /* annotations:reserved-word */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `show` ( /* annotations:reserved-word */
  id int unsigned NOT NULL primary key,
  a int unsigned DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DELIMITER //
CREATE DEFINER=`root`@`127.0.0.1` PROCEDURE `order`(a int) /* annotations: has-routine, reserved-word */
    DETERMINISTIC
BEGIN
	INSERT INTO orders(id) VALUES (a);
END//
DELIMITER ;
