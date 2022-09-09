# This file is used for testing temp-schema workspace cleanup and MDL conflicts.

CREATE DATABASE _skeema_tmp;
USE _skeema_tmp;


CREATE TABLE a (
    id int unsigned not null auto_increment,
    p1_id bigint(20) unsigned NOT NULL,
    PRIMARY KEY (id),
    KEY p1 (p1_id),
    CONSTRAINT fkap1 FOREIGN KEY (p1_id) REFERENCES parent_side.p1 (id)
);

CREATE TABLE b (
    id int unsigned not null auto_increment,
    p2_id bigint(20) unsigned NOT NULL,
    PRIMARY KEY (id),
    KEY p2 (p2_id),
    CONSTRAINT fkbp2 FOREIGN KEY (p2_id) REFERENCES parent_side.p2 (id)
);

CREATE TABLE c (
    id int unsigned not null auto_increment,
    p1_id bigint(20) unsigned NOT NULL,
    p2_id bigint(20) unsigned NOT NULL,
    PRIMARY KEY (id),
    KEY p1 (p1_id),
    KEY p2 (p2_id),
    CONSTRAINT fkcp1 FOREIGN KEY (p1_id) REFERENCES parent_side.p1 (id),
    CONSTRAINT fkcp2 FOREIGN KEY (p2_id) REFERENCES parent_side.p2 (id)
);
