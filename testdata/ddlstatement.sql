use analytics
CREATE TABLE widget_counts (
  name varchar(40) NOT NULL,
  cnt int unsigned,
  PRIMARY KEY (name)
) ENGINE=InnoDB;
DROP TABLE activity;
ALTER TABLE rollups DROP COLUMN value;
ALTER TABLE pageviews DROP COLUMN domain;
INSERT INTO pageviews (url, start_ts, end_ts) VALUES ("foo.html", 123, 456);

