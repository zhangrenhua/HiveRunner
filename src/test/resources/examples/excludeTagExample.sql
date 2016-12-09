create database foo;
create table foo.bar (s string);

-- @EXCLUDE_FROM_TEST:BEGIN

drop database foo cascade;

-- @EXCLUDE_FROM_TEST:END

