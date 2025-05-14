CREATE;
CREATE DEFINER `not enough tokens`;
CREATE DEFINER =;
CREATE DEFINER = `somerole`;
CREATE DEFINER 'meep'@'moop' PROCEDURE `foo` SELECT 'skipped equals sign in definer';
CREATE DEFINER = 2 procedure foo select 'invalid definer name';
CREATE or RECOMBOBULATE FUNCTION funccuruserparens() RETURNS int RETURN 42;
CREATE OR REPLACE;
CREATE definer=root@localhost PROCFUNC `whoops` select 'procfunc is not a valid type of routine';
