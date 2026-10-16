# This file contains MariaDB object types that were not exported by Skeema v1
# but are now exported by Skeema v2. This is used in test coverage for logic
# that ensures DROPs are not emitted if these objects are missing from the fs
# and the generator option is a Skeema v1 value.

USE product;

CREATE TABLE sys_ver_simple (
  id int unsigned not null auto_increment,
  name varchar(80),
  points int,
  PRIMARY KEY (id)
) WITH SYSTEM VERSIONING;

CREATE TABLE sys_ver_bitemporal (
  id int unsigned not null auto_increment,
  name varchar(80),
  points int,
  start_ts TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
  end_ts TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
  onboarded DATE,
  offboarded DATE,
  PRIMARY KEY (id),
  PERIOD FOR SYSTEM_TIME(start_ts, end_ts),
  PERIOD FOR onboard(onboarded, offboarded)
) WITH SYSTEM VERSIONING;

