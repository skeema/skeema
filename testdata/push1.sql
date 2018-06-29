use analytics
DROP TABLE pageviews;
INSERT INTO rollups (metric_id, value) VALUES (444, 14890);

use product
ALTER TABLE users DROP COLUMN credits;
ALTER TABLE posts ADD COLUMN featured tinyint(1) unsigned NOT NULL;
ALTER DATABASE product CHARACTER SET utf8 COLLATE utf8_swedish_ci;

CREATE DATABASE bonus;
use bonus;
CREATE TABLE placeholder (
  id int unsigned NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;
