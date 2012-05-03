<?php

/*
 * MySQLfs PHP APIs
 *
 * 2012 - Andrea Brancatelli <andrea@brancatelli.it>
 *
 */

class MySQLfs {

    var $db;

    function MySQLfs($hostname, $database, $username, $password)
    {

        $this->db = mysql_connect( $hostname,
                                   $username,
                                   $password,
                                   true);
        mysql_select_db($database);
        
    }


    function locateNode($path, $parent = 1)
    {

        $xploded = explode("/", $path, 3);

        
        
    }

    function fetchDir($rootDir)
    {

        
        
    }

}