<?php

/*
 * MySQLfs PHP APIs
 *
 * 2012 - Andrea Brancatelli <andrea@brancatelli.it>
 *
 */



/**
 * This Data Model require PEAR MDB2
 */
require_once 'MDB2.php';

/**
 * Base class to the MySQLfs Class
 * @package MySQLfsAPI
 */
class MySQLfs {

    var $dbLink;

    /**
     * The base data model constructor. It just connects to the DB
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     */
    function __construct($hostname, $database, $username, $password)
    {

	$_CONFIG["database"] = "mysqli://".$username.":".$password."@".$hostname."/".$database;
	$_CONFIG["database_options"] = array();

        $this->dbLink =& MDB2::factory($_CONFIG["database"], $_CONFIG["database_options"]);
        if (PEAR::isError($this->dbLink)) {
           die($this->dbLink->getMessage());
        }

        $this->dbLink->setFetchMode(MDB2_FETCHMODE_ASSOC);
        
    }

    /**
     * The base data model deconstructor. It just disconnects from the DB
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     */
    function __destruct()
    {
	$this->dbLink->disconnect();
    }

    /**
     * Returns a single value (first row, first column) from a query
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     * @param string $query The query to be executed
     * @return string Fetched value
     */
    function getSingleValue($query)
    {
        $result =& $this->dbLink->query($query);
        if (PEAR::isError($result)) { die($result->getMessage()); }
        $row = $result->fetchRow(MDB2_FETCHMODE_ORDERED);
        return $row[0];
    }

    /**
     * Returns a single row (first row) from a query
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     * @param string $query The query to be executed
     * @return array Fetched row
     */
    function getSingleRow($query)
    {
        $result =& $this->dbLink->query($query);
        if (PEAR::isError($result)) { die($result->getMessage()); }
        $row = $result->fetchRow(MDB2_FETCHMODE_ASSOC);
        return $row;
    }

    /**
     * Returns a single row (first row) from a query
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     * @param string $query The query to be executed
     * @return array Fetched row
     */
    function getAllRows($query)
    {
        $result =& $this->dbLink->query($query);
        if (PEAR::isError($result)) { die($result->getMessage()); }
        $rows = $result->fetchAll(MDB2_FETCHMODE_ASSOC);
        return $rows;
    }


    /**
     * Recursively explode a path into an array with references to each parent
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     * @param string $path The path to fetch
     * @param string $parent The parent to start from (Defaults to NULL)
     * @return array The directory tree
     */
    function explodePath($path, $parent = NULL)
    {
        
	$ret = array();
        
	if ($path != "/") 
	{

                if (($parent == NULL) AND (substr($path, 0, 1) != "/"))
                    $path = "/".$path;

                $xploded = explode("/", $path, 2);
                
                if ($xploded[0] == "")
                    $xploded[0] = "/";
                
                $ret[] = $xploded[0];                    
		
                if (count($xploded) > 1)
                    $ret = array_merge($ret, $this->explodePath($xploded[1], 1));
	}
	else
	{
		$ret[] = "/";
	}

	return $ret;
    }

    /**
     * Returns an array with all the information for a specific inode
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     * @param string $name inode's name
     * @param string $parent inode's parent
     * @return array The inode structure
     */
    function locateInode($name, $parent = NULL)
    {
        
	if ($parent != NULL)
		$ret = $this->getSingleRow("SELECT * FROM tree WHERE name = '".$name."' AND parent = '".$parent."'");
	else
		$ret = $this->getSingleRow("SELECT * FROM tree WHERE name = '".$name."' AND parent IS NULL");

	return $ret;
    }

    /**
     * Create a new item (file/dir) at the specified parent
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     * @param string $name filename
     * @param string $parent the folder containing it
     * @return integer the inode of the created file
     */
    function createItem($name, $parent, $mode = "33204")
    {
        if ($parent == NULL)
            $query  = "INSERT INTO tree SET name = '".$name."', parent = NULL";
        else
            $query  = "INSERT INTO tree SET name = '".$name."', parent = ".$parent;
	$affected =& $this->dbLink->exec($query);
	if (PEAR::isError($affected)) { die($affected->getMessage()); }	

	$inodeArray =  $this->locateInode($name, $parent);
	$inode = $inodeArray["inode"];

	$query  = "INSERT INTO inodes SET inode = '".$inode."', 
					  inuse = 0,
					  deleted = 0,
					  mode = '".$mode."',
					  uid = 0,
					  gid = 0,
					  atime = UNIX_TIMESTAMP(NOW()),
					  ctime = UNIX_TIMESTAMP(NOW()),
					  mtime = UNIX_TIMESTAMP(NOW()),
					  size = 0";
	$affected =& $this->dbLink->exec($query);
	if (PEAR::isError($affected)) { die($affected->getMessage()); }	

	return $inode;
    }
    
    /**
     * Add a block of data to an inode
     * NOTE: this function DOES NOT (yet?) take in consideration
     * partially-filled blocks!
     * It should be pretty uncommon in a PHP API to append to an
     * already existing file but, hey, an API is an API, so this
     * needs to be fixed.
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     * @param integer $inode the inode to add data
     * @param string $content the data to add
     * @return TRUE or FALSE
     */
    function addBlock($inode, $content)
    {
	$seq = $this->getSingleValue("SELECT COUNT(*) FROM data_blocks WHERE inode = '".$inode."'");

	$query  = "INSERT INTO data_blocks SET data = '".addslashes($content)."', inode = '".$inode."', seq = '".$seq."'";
	$affected =& $this->dbLink->exec($query);
	if (PEAR::isError($affected)) { die($affected->getMessage()); }	

	$query  = "UPDATE data_blocks SET datalength = OCTET_LENGTH(data) WHERE inode = '".$inode."' AND seq = '".$seq."'";
	$affected =& $this->dbLink->exec($query);
	if (PEAR::isError($affected)) { die($affected->getMessage()); }	

	$size = $this->getSingleValue("SELECT SUM(datalength) FROM data_blocks WHERE inode = '".$inode."'");

	$query  = "UPDATE inodes SET size = '".$size."' WHERE inode = '".$inode."'";
	$affected =& $this->dbLink->exec($query);
	if (PEAR::isError($affected)) { die($affected->getMessage()); }	
    }

    /**
     * Returns a multidimensional array with all the files/dirs in a specific inode
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     * @param string $inode The inode to fetch
     * @return array The directory content
     */
    function fetchDir($inode)
    {

	$wholeDir = array();

	$baseDir = $this->getSingleRow("SELECT * FROM tree, inodes WHERE inodes.inode = tree.inode AND tree.inode  = '".$inode."'");
	$baseDir["name"] = ".";
	$wholeDir = $this->getAllRows("SELECT * FROM tree, inodes WHERE inodes.inode = tree.inode AND parent = '".$baseDir["inode"]."' ORDER BY tree.name");

	array_unshift($wholeDir, $baseDir);

	return $wholeDir; 
    }

    /**
     * Returns a multidimensional array with all the files/dirs in a specific path
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     * @param string $rootDir The Path to fetch
     * @return array The directory content
     */
    function fetchPath($path)
    {

	$tree = $this->explodePath($path);

	$inode = NULL;
	foreach ($tree as $branch)
	{
		$thisBranch = $this->locateInode($branch, $inode);
		$inode = $thisBranch["inode"];
	}

	return $this->fetchDir($inode);
    }

    /**
     * Returns a string containing an data block
     *
     * @author Andrea Brancatelli <andrea@brancatelli.it>
     * @package MySQLfsAPI
     * @param integer $inode The Inode to Return
     * @param integer $seq The Inode sequence to Return
     * @return string The inode content or FALSE if the requested Inode doesn't exist
     */
    function fetchDataBlock($inode, $seq)
    {

	$blockCount = $this->getSingleValue("SELECT COUNT(*) FROM data_blocks WHERE inode = '".$inode."' AND seq = '".$seq."'");
	if ($blockCount > 0)
		return $this->getSingleValue("SELECT data FROM data_blocks WHERE inode = '".$inode."' AND seq = '".$seq."'");
	else
		return FALSE;

    } 
    

    /**
     * Returns a string containing a file requested as a full path
     * Not to be used with big files as it creates a copy of the
     * file in memory. If you need to read a big file read it
     * block-by-block (see fetchDataBlock)
     *
     * @author Andrea Brancatelli <andrea@brancatelli.i>
     * @package MySQLfsAPI
     * @param string $path The file to return
     * @return string The file content
     */
     function fetchFile($path)
     {

	$tree = $this->explodePath($path);

        $inode = NULL;
        foreach ($tree as $branch)
        {
                $thisBranch = $this->locateInode($branch, $inode);
                $inode = $thisBranch["inode"];
        }

	$seq = 0;
        $ret = "";

	while ($nextBlock = $this->fetchDataBlock($inode, $seq))
	{
		$ret = $ret.$nextBlock;
		$seq++;
	}

	return $ret;

     }

     /**
      * Writes the received string in a specific path
      * Has to check if the specified path already exists.
      * 
      * @author Andrea Brancatelli <andrea@brancatelli.it>
      * @package MySQLfsAPI
      * @param string $content the file content
      * @param string $path the destination path
      * @return bool TRUE or FALSE :)
      */
     function saveFile($content, $path)
     {

        $tree = $this->explodePath($path);

        $inode = NULL;
        foreach ($tree as $branch)
        {
		$parent = $inode;
                $thisBranch = $this->locateInode($branch, $inode);
                $inode = $thisBranch["inode"];
        }

	if ($inode == "")
	{
		$inode = $this->createItem($branch, $parent, "33204");
		$block = str_split($content, 131072);
		foreach ($block as $EachBlock)
			$this->addBlock($inode, $EachBlock);
	}
	else
		return FALSE;

     }
     
     
     /**
      * Create a directory from it's original path
      * 
      * @author Andrea Brancatelli <andrea@brancatelli.it>
      * @package MySQLfsAPI
      * @param string $path the path to create
      *
      */
     function createPath($path)
     {

        $tree = $this->explodePath($path);

        $inode = NULL;
        
        foreach ($tree as $branch)
        {
                $thisBranch = $this->locateInode($branch, $inode);
                
                if ($thisBranch["inode"] == "")
                    $inode = $this->createItem($branch, $inode, "16893");
                else
                    $inode = $thisBranch["inode"];
                
        }


     }
     

}
