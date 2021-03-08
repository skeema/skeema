/* pre comment preserved   */
CREATE TABLE posts (
  id bigint unsigned NOT NULL AUTO_INCREMENT primary key,
  user_id bigint unsigned NOT NULL,
  body varchar(150),
  created_at int unsigned,
  index user_created (user_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- post comment preserved