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
  FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE SET NULL, /* annotations: has-fk */
  FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
