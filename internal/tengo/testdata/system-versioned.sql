# Coverage for system versioned tables, application-time periods, and bitemporal
# tables. All of these are MariaDB-specific.

use testing;

CREATE TABLE sys_ver_simple (
  id int unsigned not null auto_increment,
  name varchar(80),
  points int,
  PRIMARY KEY (id)
) WITH SYSTEM VERSIONING;

CREATE TABLE sys_ver_expl_period_unsupported_diff (
  id int unsigned not null auto_increment,
  name varchar(80),
  points int,
  start_ts TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
  end_ts TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
  PRIMARY KEY (id),
  PERIOD FOR SYSTEM_TIME(start_ts, end_ts)
) WITH SYSTEM VERSIONING;

CREATE TABLE sys_ver_exclude_cols_unsupported_diff (
  id int unsigned not null auto_increment,
  name varchar(80),
  points int WITHOUT SYSTEM VERSIONING,
  PRIMARY KEY (id)
) WITH SYSTEM VERSIONING;
# Note: the server also supports inverting the previous syntax by specifying
# WITH SYSTEM VERSIONING only on specific columns, but that results in SHOW
# CREATE TABLE converting to equivalent WITHOUT clauses as above, so no need
# for separate coverage.

CREATE TABLE sys_ver_inno_tx_unsupported_diff (
  id int unsigned not null auto_increment,
  name varchar(80),
  points int,
  start_tx BIGINT UNSIGNED GENERATED ALWAYS AS ROW START,
  end_tx BIGINT UNSIGNED GENERATED ALWAYS AS ROW END,
  PRIMARY KEY (id),
  PERIOD FOR SYSTEM_TIME(start_tx, end_tx)
) WITH SYSTEM VERSIONING;

# Note: this only covers one variation of system versioned partitioning syntax,
# but several others are supported by the server and should be present here once
# full introspection and diff support is implemented:
# * PARTITION BY SYSTEM_TIME LIMIT n
# * PARTITION BY SYSTEM_TIME INTERVAL i
# * PARTITION BY SYSTEM_TIME with no partition list, optionally with PARTITIONS n
# * AUTO rotation (MariaDB 10.9+)
CREATE TABLE sys_ver_partitioned_unsupported_diff (
  id int unsigned not null auto_increment,
  name varchar(80),
  points int,
  PRIMARY KEY (id)
) WITH SYSTEM VERSIONING
PARTITION BY SYSTEM_TIME (
  PARTITION ph HISTORY,
  PARTITION pc CURRENT
);

CREATE TABLE app_time_pd_unsupported_diff (
  id int unsigned not null auto_increment,
  name varchar(80),
  points int,
  onboarded DATE,
  offboarded DATE,
  PERIOD FOR onboard(onboarded, offboarded),
  PRIMARY KEY (id)
);

# Support for WITHOUT OVERLAPS in MariaDB 10.5+, see MDEV-16978
CREATE TABLE app_time_pd_wo_overlap_unsupported_diff (
  id int unsigned not null auto_increment,
  name varchar(80),
  points int,
  onboarded DATE,
  offboarded DATE,
  PERIOD FOR onboard(onboarded, offboarded),
  PRIMARY KEY (id /*M!100500 , onboard WITHOUT OVERLAPS */)
);

CREATE TABLE sys_ver_bitemporal_unsupported_diff (
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
