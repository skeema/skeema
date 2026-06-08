# Table using various MariaDB-specific column types, just to test introspection

use testing;

CREATE TABLE mrdb_coltypes (
	id int unsigned NOT NULL,
	/*M!100500 ip6 inet6, */
	/*M!101000 ip4 inet4, */
	/*M!100700 external_id uuid, */
	/*M!120300 metadata xmltype, */
	PRIMARY KEY (id)
);
