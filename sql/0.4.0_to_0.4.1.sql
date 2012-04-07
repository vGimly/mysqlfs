##
## This scripts is NOT meant to be run on a working database
##
## IT IS AN INFORMATIVE SCRIPT ONLY
##
## TO UPGRADE A RUNNING 0.4.0 FILESYSTEM TO A 0.4.1 ONE PLEASE
## CREATE A NEW MYSQLFS INSTANCE AND COPY THE DATAS FROM
## THE OLD ONE TO THE NEW ONE. PLEASE SEE THE README
## FOR INSTRUCTIONS ON THIS
##

DONTRUNME;

alter table `data_blocks` modify column `data` longblob;

alter table `data_blocks` engine=innodb;
alter table `inodes` engine=innodb;
alter table `tree` engine=innodb;

