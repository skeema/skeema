# Tables testing behavior of "hidden" linter rules, which are kept separate
# due to being overly-broad. In other words, if tested against validcfg/*.sql,
# they would add too many annotations.

CREATE TABLE bids (
	iD bigint NOT null auto_increment, /* annotations: ids */
	user_ID int unsigned, /* annotations: ids, nullable */
	auction_id bigint(20) DEFAULT NULL, /* annotations: ids, nullable */
	productID varchar(30) NOT NULL, # not tripping ids since no underscore
	product_id bigint(15) unsigned NOT NULL, # non-standard display width shouldn't trip up ids checker
	low_bid int unsigned NOT NULL, # name ending in id without underscore shouldn't trip up ids checker
	high_bid int unsigned not NULL,
	PRIMARY KEY (iD)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
