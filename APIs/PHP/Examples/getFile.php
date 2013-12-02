#!/usr/bin/php
<?php

 /*
  * MySQLfs - Sample Application
  *
  * Show the contents of the specified file
  *
  * 2012 - Andrea Brancatelli <andrea@brancatelli.it>
  *
  */

include "../MySQLfs.php";
include "./config.php";

$MySQLfs = new MySQLfs($_CONFIG["hostname"], $_CONFIG["dbname"], $_CONFIG["username"], $_CONFIG["password"], $_CONFIG["tablePrefix"]);

echo "\n";
if ($argc < 3) die ("Not enough parameters!\n\ngetFile.php <mysqlfsPath> <localPath>\n\n");

echo "Contents for path ".$argv[1]."\n";

$File = $MySQLfs->fetchFile($argv[1]);

file_put_contents($argv[2], $File);
