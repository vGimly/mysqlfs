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


