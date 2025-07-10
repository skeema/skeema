CREATE DATABASE IF NOT EXISTS _skeema_tmp;
use _skeema_tmp;
CREATE TABLE bar (id int unsigned NOT NULL PRIMARY KEY);
INSERT INTO bar (id) VALUES (1), (22), (333);