CREATE TABLE fkparent (
  id int(10) unsigned NOT NULL,
  name varchar(80) NOT NULL,
  classification varchar(40) NOT NULL,
  categories varchar(40) NOT NULL,
  spend int(10) unsigned NOT NULL,
  floor int(10) unsigned NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY name_idx (name),
  UNIQUE KEY name_class_prefix (name, classification(20)),
  UNIQUE KEY cat_spend_floor (categories, spend, floor),
  KEY name_class_non_unique (name, classification),
  KEY class_spend_idx (classification, spend)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE fkchild (
  id int(10) unsigned NOT NULL,
  name varchar(80) NOT NULL,
  classification varchar(40) NOT NULL,
  categories varchar(40) NOT NULL,
  spend int(10) unsigned NOT NULL,
  floor int(10) unsigned NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT name_id Foreign  Key(name, id) referenceS fkparent (name, id), /* annotations: has-fk,fk-parent */
    constraint `nam_clas`FOREIGN KEY (name, classification)REFERENCES `fkparent` (name,classification),/* annotations: fk-parent */
Constraint `namefk` foreign key (name) references fkparent (name),
  CONSTRAINT cat_spend FOREIGN KEY (categories, spend) references fkparent (categories, spend), /* annotations: fk-parent */
  constraint other_db_not_checked FOREIGN KEY (floor) references elsewhere.something (floor),
  constraint class_spend foreign key (classification, spend) REFERENCES fkparent (classification, spend) /* annotations: fk-parent */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
