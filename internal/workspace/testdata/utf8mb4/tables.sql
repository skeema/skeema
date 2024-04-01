# When requesting a MySQL 5.x Docker workspace on arm64, MySQL 8.0 is used
# instead, since no earlier arm64 Docker images are available. In this case
# Skeema sets a session variable to avoid the new default collation for utf8mb4
# in MySQL 8.0. These tables test various situations to confirm the behavior.
# Note that the dir's .skeema file also specifies utf8mb4 with
# utf8mb4_general_ci for the database-level defaults, which should affect tbl3.

CREATE TABLE tbl1 (
    id int unsigned NOT NULL auto_increment,
    name varchar(30) character set utf8mb4,
    PRIMARY KEY (id)
);

CREATE TABLE tbl2 (
    id int unsigned NOT NULL auto_increment,
    name varchar(30),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tbl3 (
    id int unsigned NOT NULL auto_increment,
    name varchar(30),
    PRIMARY KEY (id)
);