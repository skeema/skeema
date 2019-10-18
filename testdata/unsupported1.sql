use product
ALTER TABLE subscriptions
  ADD COLUMN subscription_id int(10) unsigned NOT NULL AUTO_INCREMENT FIRST,
  ADD KEY sub_id_user (subscription_id, user_id),
  AUTO_INCREMENT=456
  PARTITION BY RANGE (user_id)
  SUBPARTITION BY HASH(post_id)
  SUBPARTITIONS 2 (
    PARTITION p0 VALUES LESS THAN (123),
    PARTITION p1 VALUES LESS THAN MAXVALUE
  )
;