use product
CREATE TABLE _widgets (
  id int unsigned NOT NULL AUTO_INCREMENT,
  name varchar(30),
  PRIMARY KEY (id),
  UNIQUE KEY name_idx (name)
) ENGINE=InnoDB;

use analytics
CREATE TABLE _trending (
  content_id int unsigned NOT NULL,
  content_type char(10) NOT NULL,
  PRIMARY KEY (content_id, content_type)
) ENGINE=InnoDB;

CREATE DATABASE archives CHARACTER SET utf8mb4;
use archives
CREATE TABLE foo (
 id int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY
) ENGINE=InnoDB;

