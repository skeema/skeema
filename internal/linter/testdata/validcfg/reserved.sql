CREATE TABLE testcolkw ( /* annotations:reserved */
  id int unsigned NOT NULL primary key,
  SPECIAL_RESERVED_CHECKER_TEST_KEYWORD int unsigned DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE special_reserved_checker_test_keyword ( /* annotations:reserved */
  id int unsigned NOT NULL primary key,
  a int unsigned DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;