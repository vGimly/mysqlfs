
 MySQLfs - PHP API - release 0.4.2alpha
 Andrea Brancatelli <andrea@brancatelli.it>

 ######################################################################################

 This is the first release of the PHP API for MySQLfs.

 This first version allows you to:

 * locate a path in the filesystem
 * locate a file in the filesystem
 * get informations on a path
 * get informations on a file
 * retrive a file (either all-in-one or one block per time)
 * write a file

 The API was mildly tested and seems to be working.

 In the "Examples" subdir you can find some... guess what? Examples. They are general
 purpose utility that can be used from command line.

 listDirs.php - Lists files in a specific dir
 getFile.php - Get a specific file from mysqlfs 
 putfile.php - Put a specific file into mysqlfs

 In order to use them you have to copy config.dist.php in config.php and edit it to
 suit your specific DB settings.

 ######################################################################################

 Waiting for an "official" documentation for the API, a quick reference that should
 be probably enough.

 The API is contained in a class called MySQLfs. It has to be instantiated by passing
 the DB-Parameters. So:


 $MySQLfs = new MySQLfs($DatabaseHostname, $DatabaseName, $DatabaseUsername, $DatabasePassword);


 Once created the object here are the basic classes:

 
 * array MySQLfs::explodePath($path, [$parent])

   Explode a path into an array, keeping track of the correct parent.

 * array MySQLfs::locateInode($name, [$parent])

   Returns an associative array with inode, parent and name for a specific filename, in a specific parent-dir

 * inode MySQLfs::createFile($name, $parent)

   Create an empty file in the specified parent-dir. Returns the newly created inode.

 * bool MySQLfs::addBlock($inode, $content)

   Adds a block of content to a specific inode. Returns TRUE or FALSE according to whether the operation completed
   successfully

 * array-array MySQLfs::fetchDir($inode)

   Returns a multidimensional array with all the files (or subdirs) contained in a specific inode

 * array-array MySQLfs::fetchPath($path)

   Returns a multidimensional array with all the files (or subdirs) contained in a specific path

 * string MySQLfs::fetchDataBlock($inode, $seq)

   Returns a string with the content of a specific data block

 * string MySQLfs::fetchFile($path)

   Returns the file at $path into a string. Beware of big files as it's working in-memory.

 * bool MySQLfs::saveFile($content, $path)

   Write $content into a file at $path. Basically the opposite of fetchFile, same recommendation apply.




Have fun
