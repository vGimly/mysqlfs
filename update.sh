#!/bin/sh

##
##  MySQLfs - database upgrade script
##
##  rely on a "DATABASE_VERSION" table inside the database
##  requires mysql client to be in the path...
##
##  if it can't find the table it asks you if you want to create it
##
##

DBUpdateScripts="sql/update"

echo 
echo Welcome to MySQLfs database upgrade script - relase 0.4.2beta
echo
echo It's recommend to shutdown MySQLfs's instance before upgrading the DB.
echo Upgrading the DB with a working instance could corrupt your data.
echo
echo
echo In order to upgrade your DB we need some informations.
echo
echo "Please insert your DB host (usually localhost):"
read DBHost
echo "Please insert your MySQLfs DB name (usually mysqlfs):"
read DBName
echo Please insert your MySQLfs username:
read DBUser
echo Please insert your MySQLfs password:
read DBPass
echo

echo Please confirm the following settings:
echo mysql://$DBUser:$DBPass@$DBUser/$DBName
echo
echo "Correct? (y/n)"
read Correct

if [ "$Correct" != "y" ]; then
  echo OK, exiting.
  exit 1
fi


echo 
echo Checking for DATABASE_VERSION table
TableExist=`echo "SHOW TABLES LIKE 'DATABASE_VERSION';" | mysql -N -h $DBHost -u $DBUser --password=$DBPass $DBName`

if [ "$TableExists" != "DATABASE_VERSION" ]; then
  echo "DATABASE_VERSION doesn't seem to exist in your database."
  echo If this is your first database upgrade ever this may be normal.
  echo PLEASE BE AWARE THAT THIS SCRIPT REQUIRE YOUR CURRENT MYSQLFS
  echo RELEASE TO BE AT LEAST 0.4.1!!!!!!
  echo
  echo Continue creating DATABASE_VERSION?
  read Correct

  if [ "$Correct" != "y" ]; then
    echo OK, exiting.
    exit 1
  fi

  echo Executing $DBUpdateScripts/0000000.sql
  mysql -N -h $DBHost -u $DBUser --password=$DBPass $DBName < $DBUpdateScripts/0000000.sql > /tmp/dbupdate_stdout.log 2> /tmp/dbupdate_stderr.log
  echo

fi

echo
echo Checking current DATABASE_VERSION:
CurrentDB=`echo "SELECT MAX(CURRENT_VERSION) FROM DATABASE_VERSION;" | mysql -N -h $DBHost -u $DBUser --password=$DBPass $DBName`

if [ "$CurrentDB" == "NULL" ]; then
  CurrentDB=0
fi

echo Current DB Version $CurrentDB

NextDB=`expr $CurrentDB + 1`
NextFile=`echo 0000000$NextDB | rev | cut -c 1-8 | rev`

echo Checking for a $NextDB version...

while [ -f $DBUpdateScripts/$NextFile.sql ]; do
  echo "Executing $DBUpdateScripts/$NextFile.sql"
  mysql -N -h $DBHost -u $DBUser --password=$DBPass $DBName < $DBUpdateScripts/$NextFile.sql > /tmp/dbupdate_stdout.log 2> /tmp/dbupdate_stderr.log
  ErrorLevel=$?
  if [ $ErrorLevel -ne 0 ]; then
   echo "Error applying update"
   cat /tmp/dbupdate_stdout.log
   rm  /tmp/dbupdate_stdout.log
   cat /tmp/dbupdate_stderr.log
   rm  /tmp/dbupdate_stderr.log
   break
  else
   echo "INSERT INTO DATABASE_VERSION SET CURRENT_VERSION = $NextDB, LAST_CHANGE=NOW();" | mysql -N -h $DBHost -u $DBUser --password=$DBPass $DBName
   echo "Upgrading DATABASE_VERSION to $NextDB"
  fi
  rm /tmp/dbupdate_std*.log

  NextDB=`expr $NextDB + 1`
  NextFile=`echo 0000000$NextDB | rev | cut -c 1-8 | rev`
done

echo 
echo Everything done.
echo
echo Now you can upgrade your MySQLfs binaries and restart your filesystem
echo
