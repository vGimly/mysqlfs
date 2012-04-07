##
## This scripts comes without ANY warranty of any kind.
## PLEASE BACKUP YOUR DATAS FIRST
##
## This is the upgrade script from MySQLfs 0.4.0 to 0.4.1
##

alter table `data_blocks` modify column `data` longblob;

alter table `data_blocks` engine=innodb;
alter table `inodes` engine=innodb;
alter table `tree` engine=innodb;

