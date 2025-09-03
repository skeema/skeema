# Coverage for vector columns and indexes in MariaDB 11.7

use testing;

CREATE TABLE vec1 (
  id int unsigned NOT NULL,
  v vector(16) NOT NULL,
  PRIMARY KEY (id),
  VECTOR KEY (v)
);

CREATE TABLE vec2 (
  id int unsigned NOT NULL,
  v vector(16) NOT NULL,
  PRIMARY KEY (id),
  VECTOR KEY (v) M=10
);

CREATE TABLE vec3 (
  id int unsigned NOT NULL,
  v vector(16) NOT NULL,
  score int,
  PRIMARY KEY (id),
  VECTOR KEY (v) comment 'hello world' IGNORED `DISTANCE`=cosine m="8",
  constraint `vec3check` CHECK (score > 0 AND score <= 100)
);

