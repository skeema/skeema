CREATE;
CREATE DEFINER `not enough tokens`;
CREATE DEFINER 'meep'@'moop' PROCEDURE `foo` SELECT 'skipped equals sign in definer';
CREATE DEFINER = root at localhost procedure foo select 'at sign instead of @ in definer';
CREATE or RECOMBOBULATE FUNCTION funccuruserparens() RETURNS int RETURN 42;
CREATE OR REPLACE;
CREATE definer=root@localhost PROCFUNC `whoops` select 'procfunc is not a valid type of routine';
