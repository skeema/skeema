# Table testing behavior of spatial indexes with and without SRIDs in MySQL 8.
# Also covers traditional duplicate index detection for spatial indexes.
CREATE TABLE spatials (
	id int unsigned NOT NULL,
	g1 geometry NOT NULL,
	g2 geometry NOT NULL SRID 0,
	g3 geometry NOT NULL SRID 4326,
	PRIMARY KEY (id),
	SPATIAL INDEX s1 (g1), /* annotations: dupe-index */
	SPATIAL INDEX s2 (g2),
	SPATIAL INDEX s3 (g3),
	SPATIAL INDEX s2a (g2)  /* annotations: dupe-index */
) ENGINE=InnoDB;
