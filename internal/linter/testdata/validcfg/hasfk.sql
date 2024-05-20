CREATE TABLE customers (
  id int(10) unsigned NOT NULL,
  name varchar(80) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE hasfk (
  id int(10) unsigned NOT NULL,
  customer_id int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (id),
  KEY customer (customer_id),
  CONSTRAINT customer_fk FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE SET NULL /* annotations: has-fk */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE hasfks (
  id int unsigned NOT NULL,
  customer_id int unsigned DEFAULT NULL,
  product_id int unsigned,
  PRIMARY KEY (id),
  KEY customer (customer_id),
  KEY product (product_id),
  CONSTRAINT custid FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE SET NULL, /* annotations: has-fk */
  CONSTRAINT prodid FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE /* annotations: fk-parent */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
