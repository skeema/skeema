use product;
ALTER TABLE users ADD COLUMN foo int;
ALTER TABLE subscriptions DROP COLUMN subscribed_at;
ALTER TABLE comments ENGINE=MyISAM, ADD INDEX user (user_id) USING BTREE;
CREATE TABLE foobar (id int unsigned NOT NULL);
