USE product;

CREATE TABLE many_permutations1 (
	a char(10),
	b char(10) CHARACTER SET latin1,
	c char(10) COLLATE latin1_swedish_ci,
	d char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	e char(10) COLLATE latin1_general_ci,
	f char(10) CHARACTER SET utf8mb4,
	g char(10) COLLATE utf8mb4_general_ci
) DEFAULT CHARSET=latin1;

CREATE TABLE many_permutations2 (
	a char(10),
	b char(10) CHARACTER SET latin1,
	c char(10) COLLATE latin1_swedish_ci,
	d char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	e char(10) COLLATE latin1_general_ci,
	f char(10) CHARACTER SET utf8mb4,
	g char(10) COLLATE utf8mb4_general_ci
) DEFAULT CHARSET=latin1 COLLATE latin1_general_ci;

CREATE TABLE many_permutations3 (
	a char(10),
	b char(10) CHARACTER SET latin1,
	c char(10) COLLATE latin1_swedish_ci,
	d char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	e char(10) COLLATE utf8_general_ci,
	f char(10) CHARACTER SET utf8mb3,
	g char(10) COLLATE utf8_unicode_ci
) DEFAULT CHARSET=utf8;

CREATE TABLE many_permutations4 (
	a char(10),
	b char(10) CHARACTER SET latin1,
	c char(10) COLLATE latin1_swedish_ci,
	d char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
	e char(10) COLLATE utf8_general_ci,
	f char(10) CHARACTER SET utf8mb3,
	g char(10) COLLATE utf8_unicode_ci
) DEFAULT CHARSET=utf8mb3 COLLATE utf8_unicode_ci;
