CREATE TABLE foo (
    id int unsigned not null,
    primary key (id)
);

DELIMITER //
CREATE PROCEDURE foo(name varchar(10))
BEGIN
	BEEP BOOP BLOP this is invalid sql but the ignore-proc will handle it
END
//
delimiter ;

CREATE FUNCTION foo() RETURNS int RETURN 42;

