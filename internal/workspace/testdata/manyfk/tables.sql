# The new data dictionary in MySQL 8.0 can be problematic when concurrently
# creating tables with foreign keys that point to each other; this can randomly
# result in deadlocks. This set of tables is designed to trigger this condition,
# which the workspace package must work around.

CREATE TABLE tbla (
	id int unsigned not null,
	b_id int unsigned not null,
	c_id int unsigned not null,
	primary key (id),
	foreign key fka1 (b_id) references tblb (id),
	foreign key fka2 (c_id) references tblc (id)
) ENGINE=InnoDB;

CREATE TABLE tblb (
	id int unsigned not null,
	c_id int unsigned not null,
	d_id int unsigned not null,
	primary key (id),
	foreign key fkb1 (c_id) references tblc (id),
	foreign key fkb2 (d_id) references tbld (id)
) ENGINE=InnoDB;

CREATE TABLE tblc (
	id int unsigned not null,
	d_id int unsigned not null,
	e_id int unsigned not null,
	primary key (id),
	foreign key fkc1 (d_id) references tbld (id),
	foreign key fkc2 (e_id) references tble (id)
) ENGINE=InnoDB;

CREATE TABLE tbld (
	id int unsigned not null,
	e_id int unsigned not null,
	f_id int unsigned not null,
	primary key (id),
	foreign key fkd1 (e_id) references tble (id),
	foreign key fkd2 (f_id) references tblf (id)
) ENGINE=InnoDB;

CREATE TABLE tble (
	id int unsigned not null,
	f_id int unsigned not null,
	a_id int unsigned not null,
	primary key (id),
	foreign key fke1 (f_id) references tblf (id),
	foreign key fke2 (a_id) references tbla (id)
) ENGINE=InnoDB;

CREATE TABLE tblf (
	id int unsigned not null,
	a_id int unsigned not null,
	b_id int unsigned not null,
	primary key (id),
	foreign key fkf1 (a_id) references tbla (id),
	foreign key fkf2 (b_id) references tblb (id)
) ENGINE=InnoDB;

