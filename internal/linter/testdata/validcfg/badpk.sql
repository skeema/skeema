CREATE TABLE badpk (
 a varchar(255) not null primary key, /* annotations: pk-type */
 b int not null
);

CREATE TABLE badpk_composite ( /* annotations: pk-type */
 a int not null,
 b varchar(255) not null, 
 c int not null,
 primary key (a,b)
);

CREATE TABLE goodpk (
 a varbinary(255) not null primary key,
 b int not null
);

CREATE TABLE goodpk_composite (
 a int not null,
 b varbinary(255) not null, 
 c int not null,
 primary key (a,b)
);

