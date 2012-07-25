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
if ($argc < 3) die ("Not enough parameters!\n\nputFile.php <localFile> <remoteName>\n\n");

echo "Copying ".$argv[1]." to ".$argv[2]."\n";

$Content = file_get_contents($argv[1]);

$Status = $MySQLfs->saveFile($Content, $argv[2]);

