use product
ALTER TABLE posts ADD COLUMN status varchar(20) DEFAULT 'published';
DROP TABLE comments;

use analytics
CREATE TABLE widget_counts (
  name varchar(40) NOT NULL,
  cnt int unsigned,
  PRIMARY KEY (name)
) ENGINE=InnoDB;
ALTER DATABASE analytics CHARACTER SET utf8 COLLATE utf8_swedish_ci;

CREATE DATABASE archives CHARACTER SET utf8mb4;
use archives
CREATE TABLE foo (
  id int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY
) ENGINE=InnoDB;

