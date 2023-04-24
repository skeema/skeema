CREATE TABLE badpk (
 a varchar(100) not null primary key, /* annotations: pk-type */
 b int not null
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE badpk_composite ( 
 a int not null,
 b varchar(100) not null,  /* annotations: pk-type */
 c int not null,
 primary key (a,b)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE goodpk (
 a varbinary(255) not null primary key,
 b int not null
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE goodpk_composite (
 a int not null,
 b varbinary(255) not null, 
 c int not null,
 primary key (a,b)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

