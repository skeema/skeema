use product
ALTER TABLE posts 
	ADD COLUMN byline varchar(30) NOT NULL,
	DROP KEY user_created,
	ADD KEY user_created (user_id, byline, created_at);
ALTER TABLE users ADD KEY idname (id, name);
ALTER TABLE posts ADD CONSTRAINT user_fk FOREIGN KEY (user_id) REFERENCES users (id);
