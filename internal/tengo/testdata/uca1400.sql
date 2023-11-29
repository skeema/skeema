# Coverage for MariaDB 10.10+'s new UCA-14.0.0 collations, which are represented
# differently in information_schema.collations than all previous collations;
# for background see https://jira.mariadb.org/browse/MDEV-27009

USE testcharcoll;

CREATE TABLE mdb_uca14_1 (
	a varchar(10) CHARACTER SET utf8mb4 COLLATE uca1400_as_ci,
	b char(14) CHARACTER SET utf8mb3 COLLATE uca1400_ai_ci
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE mdb_uca14_2 (
	name varchar(80),
	title_code char(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
