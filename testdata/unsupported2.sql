use product
ALTER TABLE comments ADD KEY user (user_id) USING BTREE, engine = myisam;
