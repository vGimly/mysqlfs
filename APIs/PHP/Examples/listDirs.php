#!/usr/bin/php
<?php

 /*
  * MySQLfs - Sample Application
  *
  * List the contents of the specified directory
  * (or root if unspecified)
  *
  * 2012 - Andrea Brancatelli <andrea@brancatelli.it>
  *
  */

include "../MySQLfs.php";
include "./config.php";

$MySQLfs = new MySQLfs($_CONFIG["hostname"], $_CONFIG["dbname"], $_CONFIG["username"], $_CONFIG["password"]);

echo "Directory content for path ".$argv[1]."\n";

$Root = $MySQLfs->fetchPath($argv[1]);

foreach ($Root as $EachEntry)
{

 echo str_pad($EachEntry["name"], 40);
 echo str_pad($EachEntry["size"], 5, " ", STR_PAD_LEFT)." bytes   ";
 echo strftime("%c ", $EachEntry["ctime"]);
 echo "\n";

}

