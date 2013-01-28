--create the test database

create database "king.test" from dbc as permanent = 500000;

create table test1 (t1 integer);
create table "test2.test2"("t2.t2" integer);
create table "king.test".test3(t3 integer);
create table "king.test".test4(t4 varchar(10));
create view testView as select t1 from test1;



---  clear tables before testing import 

drop table test1;
drop table "test2.test2";
drop table "king.test".test3;
drop table "king.test".test4;
drop view testView ;


delete from dbc.CostProfileTypes;
delete from dbc.CostProfiles;
delete from dbc.ConstantValues;
delete from dbc.ConstantDefs;

delete from SystemFE.Opt_Cost_Table;
delete from SystemFE.Opt_DBSCtl_Table;
delete from SystemFE.Opt_RAS_Table;

