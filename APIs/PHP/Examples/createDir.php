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

$MySQLfs = new MySQLfs($_CONFIG["hostname"], $_CONFIG["dbname"], $_CONFIG["username"], $_CONFIG["password"]);

echo "\n";
if ($argc < 2) die ("Not enough parameters!\n\ncreateDir.php <path>\n\n");

echo "Creating directory ".$argv[1]."\n";

$Status = $MySQLfs->createPath($argv[1]);

