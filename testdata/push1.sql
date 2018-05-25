use analytics
CREATE TABLE widget_counts (
  name varchar(40) NOT NULL,
  cnt int unsigned,
  PRIMARY KEY (name)
) ENGINE=InnoDB;
INSERT INTO widget_counts (name, cnt) VALUES ('foobar', 123);
ALTER TABLE pageviews DROP COLUMN domain;
ALTER TABLE activity ADD COLUMN rolled_up tinyint(1) unsigned NOT NULL;
ALTER DATABASE analytics CHARACTER SET utf8 COLLATE utf8_swedish_ci;

CREATE DATABASE bonus;
use bonus;
CREATE TABLE placeholder (
  id int unsigned NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;
