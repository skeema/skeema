CREATE TABLE one (
	id int unsigned NOT NULL,
	name varchar(100) default 'unknown',
	PRIMARY KEY (id)
);

-- CREATE...SELECT is auto-ignored
CREATE TABLE `two` AS SELECT * FROM `one`;

CREATE TABLE `three` (
	id int
);

-- system versioning is auto-ignored
CREATE TABLE four (
	name varchar(30)
) with /*lol*/ SYSTEM -- hmm
	versionING;

create table fourfour (
	beat int default 4
);

create SUPERTABLE fourfourfour (
	this is intentionally not a valid statement
);

-- bitemporal system versioning is auto-ignored
create TABLE five (
	mycounter int,
	apptime1 date,
	apptime2 date,
	row_start timestamp(6) as row start invisible,
	row_end timestamp(6) as row end invisible,
	PERIOD FOR application_time(apptime1, apptime2),
	PERIOD FOR system_time(row_start, row_end)
) with system versioning;

-- incorrect delimiter usage in a CREATE TABLE causes the statement to be
-- ignored, to avoid risk when using multiStatements
DELIMITER //
CREATE TABLE six (
	id int
);
DROP DATABASE my_important_db//
DELIMITER ;
