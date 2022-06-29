/*
  mysqlfs - MySQL Filesystem
  Copyright (C) 2006 Tsukasa Hamano <code@cuspy.org>
  Copyright (C) 2006,2007 Michal Ludvig <michal@logix.cz>
  Copyright (C) 2012-2020 Andrea Brancatelli <andrea@brancatelli.it>
  $Id$

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

#include "Config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <libgen.h>

#include <fuse/fuse.h>

#include <mysql/mysql.h>
#include <sys/xattr.h>

#include "mysqlfs.h"
#include "query.h"
#include "log.h"

#define SQL_MAX 10240
#define INODE_CACHE_MAX 4096

struct table_names *tables;

static inline int lock_inode(MYSQL *mysql, long inode)
{
    // TODO
    return 0;
}

static inline int unlock_inode(MYSQL *mysql, long inode)
{
    // TODO
    return 0;
}

static struct data_blocks_info *
fill_data_blocks_info(struct data_blocks_info *info, size_t size, off_t offset)
{
    info->seq_first = offset / DATA_BLOCK_SIZE;
    info->offset_first = offset % DATA_BLOCK_SIZE;

    unsigned long  nr_following_blocks = ((info->offset_first + size) / DATA_BLOCK_SIZE);	
    info->length_first = nr_following_blocks > 0 ? DATA_BLOCK_SIZE - info->offset_first : size;

    info->seq_last = info->seq_first + nr_following_blocks;
    info->length_last = (info->offset_first + size) % DATA_BLOCK_SIZE;
    /* offset in last block (if it's a different one from the first block) 
     * is always 0 */

    return info;
}

/**
 * Get the attributes of an inode, filling in a struct stat.  This function
 * uses query_inode_full() to get the inode and nlinks of the given path, then
 * reads the inode data from the database, storing this data into the provided
 * structure.
 *
 * @return 0 if successful
 * @return -EIO if the result of mysql_query() is non-zero
 * @return -ENOENT if the inode at the give path is not found (actually, if the number of results is not exactly 1)
 * @param mysql handle to connection to the database
 * @param path pathname to check
 * @param stbuf struct stat to fill with the inode contents
 */
int query_getattr(MYSQL *mysql, const char *path, struct stat *stbuf)
{
    int ret;
    long inode, nlinks;
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;
    ret = query_inode_full(mysql, path, NULL, 0, &inode, NULL, &nlinks);
    if (ret < 0)
      return ret;

    snprintf(sql, SQL_MAX,
             "SELECT inode, mode, uid, gid, atime, mtime "
             "FROM %s WHERE inode=%ld",
             tables->inodes, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "ERROR: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) != 1){
        mysql_free_result(result);
        return -ENOENT;
    }
    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
    }

    stbuf->st_ino = inode;
    stbuf->st_mode = atoi(row[1]);
    stbuf->st_uid = atol(row[2]);
    stbuf->st_gid = atol(row[3]);
    stbuf->st_atime = atol(row[4]);
    stbuf->st_mtime = atol(row[5]);
    stbuf->st_nlink = nlinks;
    stbuf->st_blksize = DATA_BLOCK_SIZE;

    mysql_free_result(result);

    return 0;
}

/**
 * Walk the directory tree to find the inode at the given absolute path,
 * storing name, inode, parent inode, and number of links.  Last developer of
 * this function indicates that the pathname may overflow -- sounds like a
 * good testcase :)
 *
 * If any of the name, inode, parent, or nlinks are given, those values will be
 * recorded form the inode data to the given buffers.  The name is written to
 * the given name_len.
 *
 * @return 0 if successful
 * @return -EIO if the result of mysql_query() is non-zero
 * @return -ENOENT if the file at this path is not found
 * @param mysql handle to connection to the database
 * @param path (absolute) pathname of inode to find
 * @param name destination to record (relative) name of the inode (may be NULL)
 * @param name_len length of destination buffer "name"
 * @param inode where to write the inode value, if found (may be NULL)
 * @param parent where to write the parent's inode value, if found (may be NULL)
 * @param nlinks where to write the number of links to the inode, if found (may be NULL)
 */
int query_inode_full(MYSQL *mysql, const char *path, char *name, size_t name_len,
		      long *inode, long *parent, long *nlinks)
{
    long ret;
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;

    int depth = 0;
    char *pathptr = strdup(path), *pathptr_saved = pathptr;
    char *nameptr, *saveptr = NULL;
    char sql_from[SQL_MAX], sql_where[SQL_MAX];
    char *sql_from_end = sql_from, *sql_where_end = sql_where;
    char esc_name[PATH_MAX * 2];

    // TODO: Handle too long or too nested paths that don't fit in SQL_MAX!!!
    sql_from_end += snprintf(sql_from_end, SQL_MAX, "%s AS t0", tables->tree);
    sql_where_end += snprintf(sql_where_end, SQL_MAX, "t0.parent IS NULL");
    while ((nameptr = strtok_r(pathptr, "/", &saveptr)) != NULL) {
        if (depth++ == 0) {
	  pathptr = NULL;
	}

    mysql_real_escape_string(mysql, esc_name, nameptr, strlen(nameptr));
	sql_from_end += snprintf(sql_from_end, SQL_MAX, " JOIN %s AS t%d ON t%d.inode = t%d.parent",
		 tables->tree, depth, depth-1, depth);
	sql_where_end += snprintf(sql_where_end, SQL_MAX, " AND t%d.name = '%s'",
		 depth, esc_name);
    }
    free(pathptr_saved);

    // TODO: Only run subquery when pointer to nlinks != NULL, otherwise we don't need it.
    if (nlinks != NULL) {
        snprintf(sql, SQL_MAX, "SELECT t%d.inode, t%d.name, t%d.parent, "
                        "       (SELECT COUNT(inode) FROM %s AS t%d WHERE t%d.inode=t%d.inode) "
                        "               AS nlinks "
                        "FROM %s WHERE %s",
                    depth, depth, depth, 
                    tables->tree, depth+1, depth+1, depth,
                    sql_from, sql_where);
    }
    else
    {
        snprintf(sql, SQL_MAX, "SELECT t%d.inode, t%d.name, t%d.parent, 1 AS nlinks "
        	     		   "FROM %s WHERE %s",
        	     depth, depth, depth, 
        	     sql_from, sql_where);
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "ERROR: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) != 1){
        mysql_free_result(result);
        return -ENOENT;
    }

    row = mysql_fetch_row(result);
    if(!row){
        log_printf(LOG_ERROR, "ERROR: mysql_fetch_row()\n");
        return -EIO;
    }
    log_printf(LOG_D_OTHER, "query_inode(path='%s') => %s, %s, %s, %s\n",
	       path, row[0], row[1], row[2], row[3]);
    
    if (inode)
        *inode = atol(row[0]);
    if (name)
        snprintf(name, name_len, "%s", row[1]);
    if (parent)
        *parent = row[2] ? atol(row[2]) : -1;	/* parent may be NULL */
    if (nlinks)
        *nlinks = atol(row[3]);

    mysql_free_result(result);

    return 0;
}

/**
 * Get the inode of a pathname.  This is really a convenience function wrapping
 * the query_inode_full() function, but can instead be used as a function with
 * a nestable return value.
 *
 * @return ID of inode
 * @return < 0 result of query_inode_full() if that ufnction reports a failure
 * @param mysql handle to connection to the database
 * @param path (full) pathname of inode to find
 */
long query_inode(MYSQL *mysql, const char *path)
{
    long inode, ret;
    
    ret = query_inode_full(mysql, path, NULL, 0, &inode, NULL, NULL);
    if (ret < 0)
      return ret;
    return inode;
}



/**
 * Change the length of a file, truncating any additional data blocks and
 * immediately deleting the data blocks past the truncation length.  Function
 * works by deleting whole blocks past the truncation point, limiting the
 * partially-cleared block, and zeroing the extra part of the buffer.
 * Called by mysqlfs_truncate().
 *
 * @see http://linux.die.net/man/2/truncate
 *
 * @return 0 on success; non-zero return of mysql_query() on error
 * @param mysql handle to connection to the database
 * @param path pathname of file to truncate
 * @param length new length of file
 */
int query_truncate(MYSQL *mysql, const char *path, off_t length)
{
    int ret;
    char sql[SQL_MAX];
    struct data_blocks_info info;

    fill_data_blocks_info(&info, length, 0);

    long inode = query_inode(mysql, path);
    if (inode < 0)
      return inode;

    lock_inode(mysql, inode);

    /* Start a transaction */
    ret = mysql_query(mysql, "BEGIN");

    snprintf(sql, SQL_MAX,
             "DELETE FROM %s WHERE inode=%ld AND seq > %ld",
	     tables->data_blocks, inode, info.seq_last);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    if ((ret = mysql_query(mysql, sql))) goto err_out;

    snprintf(sql, SQL_MAX,
             "UPDATE %s SET data=RPAD(data, %zu, '\\0') "
	     "WHERE inode=%ld AND seq=%ld",
             tables->data_blocks, info.length_last, inode, info.seq_last);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    if ((ret = mysql_query(mysql, sql))) goto err_out;

    snprintf(sql, SQL_MAX,
             "UPDATE %s SET datalength=OCTET_LENGTH(data) "
	     "WHERE inode=%ld AND seq=%ld",
             tables->data_blocks, inode, info.seq_last);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    if ((ret = mysql_query(mysql, sql))) goto err_out;

    snprintf(sql, SQL_MAX,
             "UPDATE %s SET size=%ld WHERE inode=%ld",
             tables->inodes, length, inode);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    if ((ret = mysql_query(mysql, sql))) goto err_out;

    /* Close the transaction */
    ret = mysql_query(mysql, "COMMIT");

    unlock_inode(mysql, inode);

    return 0;

err_out:
    /* Rollback the transaction */
    ret = mysql_query(mysql, "ROLLBACK");
    unlock_inode(mysql, inode);
    log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
    return ret;
}

/**
 * The opposite of query_rmdirentry(), this function creates a directory in
 * the tree with given inode and parent inode.
 *
 * @return 0 if successful
 * @return -EIO if the result of mysql_query() is non-zero
 * @param mysql handle to connection to the database
 * @param inode inode of new directory
 * @param name name (relative)  of directory to create
 * @param parent inode of directory holding the directory
 */
int query_mkdirentry(MYSQL *mysql, long inode, const char *name, long parent)
{
    int ret;
    char sql[SQL_MAX];
    char esc_name[PATH_MAX * 2];

    mysql_real_escape_string(mysql, esc_name, name, strlen(name));
    snprintf(sql, SQL_MAX,
             "INSERT INTO %s (name, parent, inode) VALUES ('%s', %ld, %ld)",
             tables->tree, esc_name, parent, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret) {
      log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
      return -EIO;
    }

    return 0;
}

/**
 * The opposite of query_mkdirentry(), this function deletes a directory from the tree with a parent that matches the inode given.
 *
 * @return 0 if successful
 * @return -EIO if the result of mysql_query() is non-zero
 * @param mysql handle to connection to the database
 * @param name name (relative)  of directory to delete
 * @param parent inode of directory holding the directory
 */
int query_rmdirentry(MYSQL *mysql, const char *name, long parent)
{
    int ret;
    char sql[SQL_MAX];
    char esc_name[PATH_MAX * 2];
    MYSQL_RES* result;
    MYSQL_ROW row;

    mysql_real_escape_string(mysql, esc_name, name, strlen(name));

    snprintf(sql, SQL_MAX,
             "SELECT COUNT(*) FROM %s AS t0 JOIN %s AS t1 ON t0.parent = t1.inode WHERE t1.name='%s' AND t1.parent = %ld",
             tables->tree, tables->tree, esc_name, parent);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) != 1 || mysql_num_fields(result) != 1){
        mysql_free_result(result);
        return -EIO;
    }

    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
    }

    /* The folder contains something, so we return ERROR */
    if(atoll(row[0])){
        log_printf(LOG_INFO, "Directory not empty: %lld file(s) found\n", atoll(row[0]));
        return -ENOTEMPTY;
    }else{
        log_printf(LOG_DEBUG, "Directory is empty: %lld files found\n", atoll(row[0]));
    }

    mysql_free_result(result);
    
    snprintf(sql, SQL_MAX,
             "DELETE FROM %s WHERE name='%s' AND parent=%ld",
             tables->tree, esc_name, parent);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret) {
      log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
      return -EIO;
    }

    return 0;
}

/**
 * Create an inode.  This function creates a child entry of the specified dev_t
 * type and mode in the "parent" directory given as the "parent".  Any parent
 * directory information (ie "dirname(path)") is stripped out, leaving only
 * the base pathname, but it has to be there (perhaps a bug?) since this
 * function wants to strip out the path information that might conflict with
 * the parent node's pathname.
 *
 * @see http://linux.die.net/man/2/mknod
 *
 * @return ID of new inode, or -ENOENT if the path contains no parent directory "/"
 * @param mysql handle to connection to the database
 * @param path name of directory to create
 * @param mode access mode of new directory
 * @param rdev type of inode to create
 * @param parent inode of directory holding files (parent inode)
 * @param alloc_data (unused)
 */
long query_mknod(MYSQL *mysql, const char *path, mode_t mode, dev_t rdev,
                long parent, int alloc_data)
{
    int ret;
    char sql[SQL_MAX];
    long new_inode_number = 0;
    char *name, esc_name[PATH_MAX * 2];

    if (path[0] == '/' && path[1] == '\0')  {
        snprintf(sql, SQL_MAX,
                 "INSERT INTO %s (name, parent) VALUES ('/', NULL)", tables->tree);

        log_printf(LOG_D_SQL, "sql=%s\n", sql);
        ret = mysql_query(mysql, sql);
        if(ret)
          goto err_out;
    } else {
        name = strrchr(path, '/');
        if (!name || *++name == '\0') 
            return -ENOENT;
            
        mysql_real_escape_string(mysql, esc_name, name, strlen(name));
        snprintf(sql, SQL_MAX,
                 "INSERT INTO %s (name, parent) VALUES ('%s', %ld)",
                 tables->tree, esc_name, parent);

        log_printf(LOG_D_SQL, "sql=%s\n", sql);
        ret = mysql_query(mysql, sql);
        if(ret)
          goto err_out;
    }

    new_inode_number = mysql_insert_id(mysql);

    snprintf(sql, SQL_MAX,
             "INSERT INTO %s (inode, mode, uid, gid, atime, ctime, mtime)"
             "VALUES(%ld, %d, %d, %d, UNIX_TIMESTAMP(NOW()), "
	            "UNIX_TIMESTAMP(NOW()), UNIX_TIMESTAMP(NOW()))",
             tables->inodes, new_inode_number, mode,
	     fuse_get_context()->uid, fuse_get_context()->gid);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret)
      goto err_out;

    return new_inode_number;

err_out:
    log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
    return ret;
}

/**
 * Create a directory.  This is really a wrapper to a specific invocation of query_mknod().
 *
 * @see http://linux.die.net/man/2/mkdir
 *
 * @return ID of new inode, or -ENOENT if the path contains no parent directory "/"
 * @param mysql handle to connection to the database
 * @param path name of directory to create
 * @param mode access mode of new directory
 * @param parent inode of directory holding files (parent inode)
 */
long query_mkdir(MYSQL *mysql, const char *path, mode_t mode, long parent)
{
    return query_mknod(mysql, path, S_IFDIR | mode, 0, parent, 0);
}

/**
 * Read a directory.  This is done by listing the nodes with a given node as
 * parent, calling the filler parameter (pointer-to-function) for each item.
 * The set of results is not ordered, so results would be in the "natural order"
 * of the database.
 *
 * for the kernel's implementation of a chmod() call in an inode on the FUSE
 * filesystem.
 *
 * @see http://linux.die.net/man/2/readdir
 *
 * @return 0 on success; -EIO on failure (non-zero return from mysql_query() function)
 * @param mysql handle to connection to the database
 * @param inode inode of directory holding files (parent inode)
 * @param buf buffer to pass to filler function
 * @param filler fuse_fill_dir_t function-pointer used to process each directory entry
 */
int query_readdir(MYSQL *mysql, long inode, void *buf, fuse_fill_dir_t filler)
{
    int ret;
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;

    snprintf(sql, sizeof(sql), "SELECT name FROM %s WHERE parent = '%ld'",
             tables->tree, inode);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    while((row = mysql_fetch_row(result)) != NULL){
        filler(buf, (char*)basename(row[0]), NULL, 0);
    }

    mysql_free_result(result);

    return ret;
}

/**
 * Change the mode attribute in the inode entry.  Should be the entry-point
 * for the kernel's implementation of a chmod() call in an inode on the FUSE
 * filesystem.
 *
 * @see http://linux.die.net/man/2/chmod
 *
 * @return 0 on success; -EIO on failure (non-zero return from mysql_query() function)
 * @param mysql handle to connection to the database
 * @param inode inode to update
 * @param mode new mode to set into the inode
 */
int query_chmod(MYSQL *mysql, long inode, mode_t mode)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
		"UPDATE %s SET mode = (((mode >> 9) << 9) | ((%d & ~ 32768) & ~16384)) WHERE inode=%ld",
		tables->inodes, mode, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

/**
 * Change the uid, gid attributes in the inode entry.  Should be
 * the entry-point for the kernel's implementation of a chown() call in an inode
 * on the FUSE filesystem.
 *
 * @see http://linux.die.net/man/2/chown
 *
 * @return 0 on success; -EIO on failure (non-zero return from mysql_query() function)
 * @param mysql handle to connection to the database
 * @param inode inode to update
 * @param uid uid to set (-1 to make no change to uid)
 * @param gid gid to set (-1 to make no change to gid)
 */
int query_chown(MYSQL *mysql, long inode, uid_t uid, gid_t gid)
{
    int ret;
    char sql[SQL_MAX];
    size_t index;

    index = snprintf(sql, SQL_MAX, "UPDATE %s SET ", tables->inodes);
    if (uid != (uid_t)-1)
    	index += snprintf(sql + index, SQL_MAX - index, 
			  "uid=%d ", uid);
    if (gid != (gid_t)-1)
    	index += snprintf(sql + index, SQL_MAX - index,
			  "%s gid=%d ", 
			  /* Insert comma if this is a second argument */
			  (uid != (uid_t)-1) ? "," : "",
			  gid);
    snprintf(sql + index, SQL_MAX - index, "WHERE inode=%ld", inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

/**
 * Change the utime attributes atime and mtime in the inode entry.  Should be
 * the entry-point for the kernel's implementation of a utime() call in an inode
 * on the FUSE filesystem.
 *
 * @see http://linux.die.net/man/2/utime
 * @see http://linux.die.net/man/2/stat
 *
 * @return 0 on success; -EIO on failure (non-zero return from mysql_query() function)
 * @param mysql handle to connection to the database
 * @param inode inode to update the atime, mtime
 * @param time utimbuf with new actime, modtime, to set into access and modification times
 */
int query_utime(MYSQL *mysql, long inode, struct utimbuf *time)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
             "UPDATE %s "
             "SET atime=%ld, mtime=%ld "
             "WHERE inode=%lu",
             tables->inodes, time->actime, time->modtime, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

/**
 * Read a number of bytes (perhaps larger than BLOCK_SIZE) at an offset from
 * a file.  The function does this by reading each block in succession, copying
 * the block contents into the target buffer.  The (offset % DATA_BLOCK_SIZE)
 * issue is handled by shifting the copy slightly.
 *
 * @return < 0 in case of errors (propagating result of write_one_block() )
 * @return > 0 number of bytes read (should equal size parameter)
 * @param mysql handle to connection to the database
 * @param inode inode of the file in question
 * @param buf the buffer to copy read bytes
 * @param size number of bytes to read
 * @param offset offset within the file to read from
 */
int query_read(MYSQL *mysql, long inode, const char *buf, size_t size,
               off_t offset)
{
    int ret;
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;
    unsigned long length = 0L, copy_len, seq;
    struct data_blocks_info info;
    char *dst = (char *)buf;
    char *src, *zeroes = alloca(DATA_BLOCK_SIZE);

    fill_data_blocks_info(&info, size, offset);

    /* Read all required blocks */
    if (info.seq_first == info.seq_last) {
        snprintf(sql, SQL_MAX,
                 "SELECT seq, data, datalength FROM %s WHERE inode=%ld AND seq=%lu",
	         tables->data_blocks, inode, info.seq_first);
    } else {
        snprintf(sql, SQL_MAX,
                 "SELECT seq, data, datalength FROM %s WHERE inode=%ld AND seq>=%lu AND seq <=%lu ORDER BY seq ASC",
	         tables->data_blocks, inode, info.seq_first, info.seq_last);
    }

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "ERROR: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    /* This is a bit tricky as we support 'sparse' files now.
     * It means not all requested blocks must exist in the
     * database. For those that don't exist we'll return
     * a block of \0 instead.  */
    row = mysql_fetch_row(result);
    memset(zeroes, 0L, DATA_BLOCK_SIZE);
    for (seq = info.seq_first; seq<=info.seq_last; seq++) {
        off_t row_seq = -1;
	size_t row_len = DATA_BLOCK_SIZE;
	char *data = zeroes;

	if (row && (row_seq = atoll(row[0])) == seq) {
	    data = row[1];
	    row_len = atoll(row[2]);
	}
	    
	if (seq == info.seq_first) {
	    if (row_len < info.offset_first)
	        goto go_away;

	    copy_len = MIN(row_len - info.offset_first, info.length_first);
	    src = data + info.offset_first;
	} else if (seq == info.seq_last) {
	    copy_len = MIN(info.length_last, row_len);
	    src = data;
	} else {
	    copy_len = MIN(DATA_BLOCK_SIZE, row_len);
	    src = data;
	}

	memcpy(dst, src, copy_len);
	dst += copy_len;
	length += copy_len;

	if (row && row_seq == seq)
	    row = mysql_fetch_row(result);
    }

go_away:
    /* Read all remaining rows */
    while (mysql_fetch_row(result));
    mysql_free_result(result);

    return length;
}

/**
 * Writes a specific block into the database
 *
 * This function takes an early bail-out if the size to write is zero, or if the total size to write exceeds the block size.
 *
 * This function checks to see if the previous block didn't exist -- in such
 * case, it then writes out a zero-length block.  The function then creates a
 * statement that has a '?' token representing the new data.  If the previous
 * data didn't exist, the function uses a "SET x == y" format; otherwise, a
 * "CONCAT (data, ?)".  The statement is snprintf'd, prepared, and executed; the
 * result produces either a 0 on success, or a -EIO on failure (with an error
 * message logged).
 *
 * @return 0 on success; -EIO on failure
 * @param mysql handle to connection to the database
 * @param inode inode to write out the data block on
 * @param seq sequence number of datablock to write
 * @param data buffer of content to write
 * @param size size_t length of data
 * @param offset what offset within the datablock to write the data
 */
static int write_one_block(MYSQL *mysql, long inode,
				 unsigned long seq,
				 const char *data, size_t size,
				 off_t offset)
{
    MYSQL_STMT *stmt;
    MYSQL_BIND bind[1];
    unsigned int mysqlerrno;
    char sql[SQL_MAX];
    size_t current_block_size = query_size_block(mysql, inode, seq);

    /* Shortcut */
    if (size == 0) return 0;

    if (offset + size > DATA_BLOCK_SIZE) {
        log_printf(LOG_ERROR, "%s(): offset(%zu)+size(%zu)>max_block(%d)\n", 
		   __func__, offset, size, DATA_BLOCK_SIZE);
	return -EIO;
    }

    /* We expect the inode is already locked for this thread by caller! */

    if (current_block_size == -ENXIO) {
        /* This data block has not yet been allocated */
        snprintf(sql, SQL_MAX,
                 "INSERT INTO %s SET inode=%ld, seq=%lu, data=''", tables->data_blocks, inode, seq);
        log_printf(LOG_D_SQL, "sql=%s\n", sql);
        if(mysql_query(mysql, sql)){
		mysqlerrno = mysql_errno(mysql);
		log_printf(LOG_ERROR, "WriteOneBlock EmptyBlock - mysql_error: %u %s\n", mysqlerrno, mysql_error(mysql));
		return -EIO;
        }

	/* If I just created the block then it must be zero */
	current_block_size = 0;
    }

    stmt = mysql_stmt_init(mysql);
    if (!stmt)
    {
        log_printf(LOG_ERROR, "WriteOneBlock - mysql_stmt_init(), out of memory\n");
	return -EIO;
    }

    memset(bind, 0, sizeof(bind));
    if (offset == 0 && current_block_size == 0) {
        snprintf(sql, SQL_MAX,
                 "UPDATE %s "
		 "SET data=? "
		 "WHERE inode=%ld AND seq=%lu",
		 tables->data_blocks, inode, seq);
    } else if (offset == current_block_size) {
        snprintf(sql, sizeof(sql),
                 "UPDATE %s "
		 "SET data=CONCAT(data, ?) "
		 "WHERE inode=%ld AND seq=%lu",
		 tables->data_blocks, inode, seq);
    } else {
        size_t pos, new_size;
        pos = snprintf(sql, sizeof(sql),
		 "UPDATE %s SET data=CONCAT(", tables->data_blocks);
	if (offset > 0)
	    pos += snprintf(sql + pos, sizeof(sql) - pos, "RPAD(IF(ISNULL(data),'', data), %ld, '\\0'),", offset);
	pos += snprintf(sql + pos, sizeof(sql) - pos, "?,");
	new_size = offset + size;
	if (offset + size < current_block_size) {
	    pos += snprintf(sql + pos, sizeof(sql) - pos, "SUBSTRING(data FROM %lu),", offset + size + 1);
	    new_size = current_block_size;
	}
	sql[--pos] = '\0';	/* Remove the trailing comma. */
	pos += snprintf(sql + pos, sizeof(sql) - pos, ") WHERE inode=%ld AND seq=%lu",
			inode, seq);
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    if (mysql_stmt_prepare(stmt, sql, strlen(sql))) {
	log_printf(LOG_ERROR, "mysql_stmt_prepare() failed: %s\n", mysql_stmt_error(stmt));
	goto err_out;
    }

    if (mysql_stmt_param_count(stmt) != 1) {
      log_printf(LOG_ERROR, "%s(): stmt_param_count=%d, expected 1\n", __func__, mysql_stmt_param_count(stmt));
      return -EIO;
    }
    bind[0].buffer_type= MYSQL_TYPE_LONG_BLOB;
    bind[0].buffer= (char *)data;
    bind[0].is_null= 0;
    bind[0].length= (unsigned long *)(void *)&size;

    if (mysql_stmt_bind_param(stmt, bind)) {
	log_printf(LOG_ERROR, "mysql_stmt_bind_param() failed: %s\n", mysql_stmt_error(stmt));
	goto err_out;
    }

    /*
    if (!mysql_stmt_send_long_data(stmt, 0, data, size))
    {
        log_printf(" send_long_data failed");
	goto err_out;
    }
    */
    if (mysql_stmt_execute(stmt)) {
	log_printf(LOG_ERROR, "WriteOneBlock - mysql_stmt_execute() failed: %u %s\n", mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
	goto err_out;
    }

    if (mysql_stmt_close(stmt))
	log_printf(LOG_ERROR, "failed closing the statement: %s\n", mysql_stmt_error(stmt));


    snprintf(sql, SQL_MAX,
             "UPDATE %s SET datalength=OCTET_LENGTH(data) "
             "WHERE inode=%ld AND seq=%ld",
             tables->data_blocks, inode, seq);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    if(mysql_query(mysql, sql)){
             mysqlerrno = mysql_errno(mysql);
             log_printf(LOG_ERROR, "WriteOneBlock Update DataLength - mysql_error: %u %s\n", mysqlerrno, mysql_error(mysql));
             return -EIO;
    }


    return size;

err_out:
	log_printf(LOG_ERROR, " %s\n", mysql_stmt_error(stmt));
	if (mysql_stmt_close(stmt))
	    log_printf(LOG_ERROR, "failed closing the statement: %s\n", mysql_stmt_error(stmt));
	return -EIO;
}

/**
 * Write a number of bytes (perhaps larger than BLOCK_SIZE) at an offset into
 * a file.  The function does this by writing the first partial block, then
 * writing successive blocks until the full @c size is written.
 *
 * @return < 0 in case of errors (propagating result of write_one_block() )
 * @return > 0 number of bytes written (should equal size parameter)
 * @param mysql handle to connection to the database
 * @param inode inode of the file in question
 * @param data the buffer of data to write
 * @param size number of bytes to write
 * @param offset offset within the file to write to
 */
int query_write(MYSQL *mysql, long inode, const char *data, size_t size,
                off_t offset)
{
    struct data_blocks_info info;
    unsigned long seq;
    unsigned int mysqlerrno;
    const char *ptr;
    char sql[SQL_MAX];
    int ret, commitret, ret_size = 0;

    fill_data_blocks_info(&info, size, offset);

    /* Start a transaction */
    commitret = mysql_query(mysql, "BEGIN");
    
    /* Handle first block */
    lock_inode(mysql, inode);
    ret = write_one_block(mysql, inode, info.seq_first, data,
			  info.length_first, info.offset_first);
    unlock_inode(mysql, inode);
    if (ret < 0)
        return ret;
    ret_size = ret;

    /* Shortcut - if last block seq is the same as first block
     * seq simply go away as it's the same block */
    if (info.seq_first != info.seq_last)
    {
     
        ptr = data + info.length_first;

        /* Handle all full-sized intermediate blocks */
        for (seq = info.seq_first + 1; seq < info.seq_last; seq++) {
                lock_inode(mysql, inode);
                ret = write_one_block(mysql, inode, seq, ptr, DATA_BLOCK_SIZE, 0);
                unlock_inode(mysql, inode);
                if (ret < 0) {
                    /* Better rollback... */
                    commitret = mysql_query(mysql, "ROLLBACK");
                    return ret;
                }
        	ptr += DATA_BLOCK_SIZE;
        	ret_size += ret;
        }

        /* Handle last block */
        lock_inode(mysql, inode);
        ret = write_one_block(mysql, inode, info.seq_last, ptr,
        			  info.length_last, 0);
        unlock_inode(mysql, inode);
        if (ret < 0) {
            /* Better rollback... */
            commitret = mysql_query(mysql, "ROLLBACK");
            return ret;
        }
        ret_size += ret;
    }

    /* Let's commit the transaction (and the size update...) */
    commitret = mysql_query(mysql, "COMMIT");

    /* Update file size */
    /* This has to be changed to better interact with replication.
	Specifically there's no need to run the update as select
	on the slave nodes. The solution for this is to put the
	result of the select into a variable and then use the variable
	to update the value of the DB. Doing this the replication
	will save the already computer value and the slave will run
	a simple update. Furthermore I have some feelings that 
	this will also avoid problems with non-deterministic
	updates.... */
    snprintf(sql, SQL_MAX,
             "SELECT SUM(datalength) INTO @iNodeSize FROM %s WHERE inode = %ld",
             tables->data_blocks, inode);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    mysql_query(mysql, sql);

    snprintf(sql, SQL_MAX,
             "UPDATE %s SET size = @iNodeSize WHERE inode = %ld",
             tables->inodes, inode);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    if(mysql_query(mysql, sql)) {
	mysqlerrno = mysql_errno(mysql);
	log_printf(LOG_ERROR, "mysql_error: %u %s\n", mysqlerrno, mysql_error(mysql));
        return -EIO;
    }

    return ret_size;
}

/**
 * Check the size of a file.  Check the value by reading the attribute stored
 * in the inode table itself.  The function does not summarize the size "live"
 * by summing the size of each data block; rather this value is updated in
 * query_fsck(), query_truncate(), write_one_block().  This trust in the
 * various write functions optimizes this function's response time and
 * reduces DB load.
 *
 * @return total size of the file at the inode as represented in the inode block
 * @param mysql handle to connection to the database
 * @param inode inode of the file in question
 */
ssize_t query_size(MYSQL *mysql, long inode)
{
    size_t ret;
    char sql[SQL_MAX];
    MYSQL_RES *result;
    MYSQL_ROW row;

    snprintf(sql, SQL_MAX, "SELECT size FROM %s WHERE inode=%ld",
             tables->inodes, inode);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) != 1 || mysql_num_fields(result) != 1){
        mysql_free_result(result);
        return -EIO;
    }

    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
    }

    if(row[0]){
        ret = atoll(row[0]);
    }else{
        ret = 0;
    }
    mysql_free_result(result);

    return ret;
}

/**
 * Returns the size of the given block (inode and sequence number).  Used only by write_one_block(), which is static, so this one can/should be static?
 *
 * @return -ENXIO if the inode/seq pair is not found (zero rows returned, implying that block doesn't exist)
 * @return -EIO if no row is returned (implying an error in the query response, signaled by mysql_fetch_row() returning NULL)
 * @return 0 if the rown is NULL (implying no result?)
 * @return 1 - DATA_BLOCK_SIZE (size of the actual block)
 * @param mysql handle to connection to the database
 * @param inode inode of the file in question
 * @param seq sequence number of datablock to check
 */
ssize_t query_size_block(MYSQL *mysql, long inode, unsigned long seq)
{
    size_t ret;
    char sql[SQL_MAX];
    MYSQL_RES *result;
    MYSQL_ROW row;

    snprintf(sql, SQL_MAX, "SELECT datalength FROM %s WHERE inode=%ld AND seq=%lu",
             tables->data_blocks, inode, seq);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) == 0) {
        mysql_free_result(result);
        return -ENXIO;
    }

    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
    }

    if(row[0]){
        ret = atoll(row[0]);
    }else{
        ret = 0;
    }
    mysql_free_result(result);

    return ret;
}

/**
 * Rename a file.  Called by mysqlfs_rename()
 *
 * @return 0 on success; -EIO if the mysql_query() is non-zero (and the error is logged)
 *
 * @see http://linux.die.net/man/2/rename
 *
 * @param mysql handle to the database
 * @param from name of file before the rename
 * @param to name of file after the rename
 */
int query_rename(MYSQL *mysql, const char *from, const char *to)
{
    int ret;
    long inode, parent_to, parent_from;
    char *tmp, *new_name, *old_name;
    char esc_new_name[PATH_MAX * 2], esc_old_name[PATH_MAX * 2];
    char sql[SQL_MAX];

    inode = query_inode(mysql, from);

    /* Lots of strdup()s follow because dirname() & basename()
     * may modify the original string. */
    tmp = strdup(from);
    parent_from = query_inode(mysql, dirname(tmp));
    free(tmp);

    tmp = strdup(from);
    old_name = basename(tmp);
    mysql_real_escape_string(mysql, esc_old_name, old_name, strlen(old_name));
    free(tmp);

    tmp = strdup(to);
    parent_to = query_inode(mysql, dirname(tmp));
    free(tmp);

    tmp = strdup(to);
    new_name = basename(tmp);
    mysql_real_escape_string(mysql, esc_new_name, new_name, strlen(new_name));
    free(tmp);

    snprintf(sql, SQL_MAX,
             "UPDATE %s "
	     "SET name='%s', parent=%ld "
	     "WHERE inode=%ld AND name='%s' AND parent=%ld ",
             tables->tree,
             esc_new_name, parent_to,
	     inode, esc_old_name, parent_from);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    /*
    if (mysql_affected_rows(mysql) < 1)
      return -ETHIS_IS_STRANGE;	/ * Someone deleted the direntry? Do we care? * /
    */

    return 0;
}

/**
 * Mark the file in-use: like a lock-manager, increment the count of users of
 * this file so that deletions at the inode level cannot result in purged data
 * while the file is in-use.
 *
 * @return 0 on success; -EIO if the mysql_query() is non-zero (and the error is logged)
 * @param mysql handle to the database
 * @param inode inode of the file that is to be marked deleted 
 * @param increment how many additional "uses" to increment in the file's inode
 */
int query_inuse_inc(MYSQL *mysql, long inode, int increment)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
             "UPDATE %s SET inuse = inuse + %d "
             "WHERE inode=%lu",
             tables->inodes, increment, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

/**
 * Purge inodes from files previously marked deleted (ie query_set_deleted() )
 * and are no longer in-use.  Called by mysqlfs_unlink() and mysqlfs_release()
 *
 * @return 0 on success; -EIO if the mysql_query() is non-zero (and the error is logged)
 * @param mysql handle to the database
 * @param inode inode of the file that is to be marked deleted 
 */
int query_purge_deleted(MYSQL *mysql, long inode)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
	     "DELETE FROM %s WHERE inode=%ld AND inuse=0 AND deleted=1",
             tables->inodes, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

/**
 * Mark the inode deleted where the name of the tree column is NULL.  This
 * allows files that are still in use to be deleted without wiping out their
 * underlying data.
 *
 * @return 0 on success; -EIO if the mysql_query() is non-zero (and the error is logged)
 * @param mysql handle to the database
 * @param inode inode of the file that is to be marked deleted
 */
int query_set_deleted(MYSQL *mysql, long inode)
{
    int ret;
    char sql[SQL_MAX];

    snprintf(sql, SQL_MAX,
	     "UPDATE %s i LEFT JOIN %s t ON i.inode = t.inode SET i.deleted=1 "
	     "WHERE i.inode = %ld AND t.name IS NULL",
             tables->inodes, tables->tree,
             inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    return 0;
}

/**
 * Clean filesystem.  Only run in pool_check_mysql_setup() if mysqlfs_opt::fsck == 1
 *
 * -# delete inodes with deleted==1
 * -# delete direntries without corresponding inode
 * -# set inuse=0 for all inodes
 * -# delete data without existing inode
 * -# synchronize inodes.size=data.LENGTH(data)
 * -# recalculate statistics table
 * -# optimize tables
 *
 * @return return from call to mysql_query()
 * @param mysql handle to database connection
 */
int query_fsck(MYSQL *mysql)
{

    /*
     query_fsck by florian wiessner (f.wiessner@smart-weblications.de)
    */
    printf("Starting fsck\n");

    // 1. delete inodes with deleted==1
    int ret;
//    int ret2;
    int result;
    char sql[SQL_MAX];
    printf("Stage 1...\n");
    snprintf(sql, SQL_MAX,
             "DELETE from %s WHERE deleted = 1", tables->inodes);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }
    // 2. - delete direntries without corresponding inode
    printf("Stage 2...\n");
    snprintf(sql, SQL_MAX, "delete from %s where inode not in (select inode from %s);", tables->tree, tables->inodes);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);

    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }



    // 3. set inuse=0 for all inodes
    printf("Stage 3...\n");
    snprintf(sql, SQL_MAX, "UPDATE %s SET inuse=0;", tables->inodes);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }


    // 4. delete data without existing inode
    printf("Stage 4...\n");
    snprintf(sql, SQL_MAX, "delete from %s where inode not in (select inode from %s);", tables->data_blocks, tables->inodes);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }


    // 5. synchronize inodes.size=data.LENGTH(data)
    printf("Stage 5...\n");
    long int inode;
    long int size;
    
    printf("Stage 5... resync datablock length cache\n");
    snprintf(sql, SQL_MAX, "UPDATE %s SET `datalength` = OCTET_LENGTH(`data`)", tables->data_blocks);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    
    printf("Stage 5... recompute inode sizes\n");
    snprintf(sql, SQL_MAX, "select inode, sum(datalength) as size from %s group by inode", tables->data_blocks);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);

    MYSQL_RES* myresult;
    MYSQL_ROW row;

    myresult = mysql_store_result(mysql);
    while ((row = mysql_fetch_row(myresult)) != NULL) {
     inode = atol(row[0]);
     size = atol(row[1]);
                     
      snprintf(sql, SQL_MAX, "update %s set size=%ld where inode=%ld;", tables->inodes, size, inode);
      log_printf(LOG_D_SQL, "sql=%s\n", sql);
      result = mysql_query(mysql, sql);

/*      if (myresult) { // something has gone wrong.. delete datablocks...

        snprintf(sql, SQL_MAX, "delete from %s where inode=%ld;", tables->inodes, inode);
        log_printf(LOG_D_SQL, "sql=%s\n", sql);
        ret2 = mysql_query(mysql, sql);

      }
*/ // skip this for now!

    }

    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
       return -EIO;
    }
    mysql_free_result(myresult);

    // 6. Recalculate statistics tables
    printf("Stage 6...\n");

    printf("Stage 6... recompute total files count\n");
    snprintf(sql, SQL_MAX, "UPDATE %s SET %s.value = (SELECT COUNT(*) FROM %s) WHERE %s.key = 'total_inodes_count'", tables->statistics, tables->statistics, tables->inodes, tables->statistics);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    printf("Stage 6... recompute total files size\n");
    snprintf(sql, SQL_MAX, "UPDATE %s SET %s.value = (SELECT SUM(size) FROM %s) WHERE %s.key = 'total_inodes_size'", tables->statistics, tables->statistics, tables->inodes, tables->statistics);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    // flush any pending result from previous queries
    for(; mysql_next_result(mysql) == 0;)
        /* do nothing */;

    // 7. Optimize general tables
    printf("Stage 7... optimizing tables\n");

    printf("Stage 7... optimizing inodes table\n");
    snprintf(sql, SQL_MAX, "OPTIMIZE TABLE %s", tables->inodes);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    myresult = mysql_store_result(mysql);
    mysql_free_result(myresult);

    // flush any pending result from previous queries
    for(; mysql_next_result(mysql) == 0;)
        /* do nothing */;

    printf("Stage 7... optimizing tree table\n");
    snprintf(sql, SQL_MAX, "OPTIMIZE TABLE %s", tables->tree);
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "Error: mysql_query()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    myresult = mysql_store_result(mysql);
    mysql_free_result(myresult);

    // flush any pending result from previous queries
    for(; mysql_next_result(mysql) == 0;)
        /* do nothing */;

    printf("fsck done!\n");
    return ret;

}



/** Statistical functions **/

/**
 * Return total inodes number
 *
 * @return total inode numers
 * @param mysql handle to connection to the database
 */
fsfilcnt_t query_total_inodes(MYSQL *mysql)
{
    size_t ret;
    char sql[SQL_MAX];
    MYSQL_RES *result;
    MYSQL_ROW row;
    fsfilcnt_t inodes;

    snprintf(sql, SQL_MAX, "SELECT CAST(%s.value AS UNSIGNED) FROM %s WHERE %s.key = 'total_inodes_count'", tables->statistics, tables->statistics, tables->statistics);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) != 1 || mysql_num_fields(result) != 1){
        mysql_free_result(result);
        return -EIO;
    }

    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
    }

    if(row[0]){
	inodes = (fsfilcnt_t)atol(row[0]);
    }else{
        inodes = 0;
    }
    mysql_free_result(result);

    return inodes;
}

/**
 * Return total data blocks
 *
 * @return total data blocks
 * @param mysql handle to connection to the database
 */
fsblkcnt_t query_total_blocks(MYSQL *mysql)
{
    size_t ret;
    char sql[SQL_MAX];
    MYSQL_RES *result;
    MYSQL_ROW row;
    fsblkcnt_t blocks;

    snprintf(sql, SQL_MAX, "SELECT CEIL(CAST(%s.value AS UNSIGNED)/%d) from %s WHERE %s.key = 'total_inodes_size'", tables->statistics, DATA_BLOCK_SIZE, tables->statistics, tables->statistics);

    ret = mysql_query(mysql, sql);
    if(ret){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }
    log_printf(LOG_D_SQL, "sql=%s\n", sql);

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "ERROR: mysql_store_result()\n");
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    if(mysql_num_rows(result) != 1 || mysql_num_fields(result) != 1){
        mysql_free_result(result);
        return -EIO;
    }

    row = mysql_fetch_row(result);
    if(!row){
        return -EIO;
    }

    if(row[0]){
        blocks = (fsblkcnt_t)atol(row[0]);
    }else{
        blocks = 0;
    }
    mysql_free_result(result);

    return blocks;
}

/**
 * Tables' name initialization
 *
 * @param prefix the prefix string
 */
void query_tablename_init(char *prefix)
{
    if (prefix == NULL) {
        prefix = "";
    }

    int prefixlength = strlen(prefix);
    tables = malloc(sizeof(struct table_names));
    tables->inodes = malloc(prefixlength + 8);        // Remember the null!
    tables->tree = malloc(prefixlength + 5);
    tables->data_blocks = malloc(prefixlength + 12);
    tables->statistics = malloc(prefixlength + 11);
    tables->xattr = malloc(prefixlength + 6);
    strcpy(tables->inodes, prefix);
    strcat(tables->inodes, "inodes");
    strcpy(tables->tree, prefix);
    strcat(tables->tree, "tree");
    strcpy(tables->data_blocks, prefix);
    strcat(tables->data_blocks, "data_blocks");
    strcpy(tables->statistics, prefix);
    strcat(tables->statistics, "statistics");
    strcpy(tables->xattr, prefix);
    strcat(tables->xattr, "xattr");

    fprintf(stderr, " ** Tree table: %s\n", tables->tree);
    fprintf(stderr, " ** Inodes table: %s\n", tables->inodes);
    fprintf(stderr, " ** Data blocks table: %s\n", tables->data_blocks);
    fprintf(stderr, " ** Statistics table: %s\n", tables->statistics);
    fprintf(stderr, " ** xAttr table: %s\n", tables->xattr);

}


/**
 * The opposite of query_setxattr(), this function deletes a eXtended attributes from the inode given.
 *
 * @return 0 if successful
 * @return -EIO if the result of mysql_query() is non-zero
 * @param mysql handle to connection to the database
 * @param name name of attribute to delete
 * @param inode inode of file/directory holding attribute
 */
int query_rmxattr(MYSQL *mysql, const char *attr, long inode)
{
    int ret;
    char sql[SQL_MAX];
    char esc_attr[PATH_MAX * 2];

    mysql_real_escape_string(mysql, esc_attr, attr, strlen(attr));
    snprintf(sql, SQL_MAX,
             "DELETE FROM %s WHERE attr='%s' AND inode=%ld",
             tables->xattr, esc_attr, inode);

    log_printf(LOG_D_SQL, "sql=%s\n", sql);
    ret = mysql_query(mysql, sql);
    if(ret) {
      log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
      return -EIO;
    }

    return 0;
}

/**
 * Gets a value of eXtended ATTRibute for the inode given.
 *
 * @return 0 if successful
 * @return -EIO if the result of mysql_query() is non-zero
 * @return -ENODATA if no attribute with given name exists on inode
 * @return -ERANGE if length of value of attribute exceeded buffer size
 * @param mysql handle to connection to the database
 * @param attr name of attribute to operate
 * @param inode inode of file/directory holding attribute
 * @param buf resulting buffer to get attribute value
 * @param sz resulting buffer size;
 *        if sz=0, functions returns buffer size needed, to hold attribute value
 */
int query_getxattr(MYSQL *mysql, const char *attr, long inode, char * buf, size_t sz)
{
    int ret;
    char sql[SQL_MAX];
    char esc_attr[PATH_MAX * 2];
    MYSQL_RES* result;
    MYSQL_ROW row;
    unsigned long * lengths;

    mysql_real_escape_string(mysql, esc_attr, attr, strlen(attr));
    snprintf(sql, SQL_MAX,
             "SELECT value FROM %s WHERE attr='%s' AND inode=%ld",
             tables->xattr, esc_attr, inode);

    ret = mysql_query(mysql, sql);
    if(ret) {
      log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
      return -EIO;
    }

    result = mysql_use_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    row = mysql_fetch_row(result);
    if (row == NULL ) ret=-ENODATA;
    else {
    lengths=mysql_fetch_lengths(result);
    if (lengths == NULL) ret=-ENODATA;
    else
    if (!sz) ret=*lengths;
    else if (*lengths > sz) ret=-ERANGE;
    else {
        ret=*lengths;
        memcpy(buf,row[0],ret);
    }}

    mysql_free_result(result);

    return ret;
}

/**
 * Gets a names of all eXtended ATTRibutes for the inode given.
 * The outout is the list is zero-terminated strings.
 *
 * @return number of bytes, written on buffer if successful
 * @return -EIO if the result of mysql_query() is non-zero
 * @return -ERANGE if length of attribute names list exceeded input buffer size
 * @param mysql handle to connection to the database
 * @param inode inode of file/directory holding attribute
 * @param buf resulting buffer to get attribute names
 * @param sz resulting buffer size in bytes;
 *        if sz=0, functions returns buffer size needed, to hold attribute list.
 */
int query_lsxattr(MYSQL *mysql, long inode, char * buf, size_t sz)
{
    int ptr=0;
    char sql[SQL_MAX];
    MYSQL_RES* result;
    MYSQL_ROW row;
    unsigned long * lengths;
    ssize_t len;

    snprintf(sql, SQL_MAX,
             "SELECT attr FROM %s WHERE inode=%ld", table->xattr,
             inode);

    if(mysql_query(mysql, sql)) {
      log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
      return -EIO;
    }

    result = mysql_store_result(mysql);
    if(!result){
        log_printf(LOG_ERROR, "mysql_error: %s\n", mysql_error(mysql));
        return -EIO;
    }

    while ((row = mysql_fetch_row(result))!=NULL) {
      lengths=mysql_fetch_lengths(result);
      len=*lengths;
      log_printf(LOG_INFO, "got_line: '%s' len=%ld sz=%ld, ptr=%ld\n", row[0], len, sz, ptr);
      if (!sz){ptr+=len+1;}
      else{
      if (len+ptr >= sz) {ptr=-ERANGE;break;}
        memcpy(buf+ptr,row[0],len);
        ptr+=len+1;
        buf[ptr-1]=0;
      }
    }
    mysql_free_result(result);

    return ptr;
}

/**
 * Sets a value of eXtended ATTRibute for the inode given.
 * The outout is the list is zero-terminated strings.
 *
 * @return 0 if successful
 * @return -EIO if the result of mysql_query() is non-zero
 * @return -ERANGE if length of attribute names list exceeded input buffer size
 * @return -EINVAL invalid flags value
 * @return -EEXIST attribute already exists and flags=XATTR_CREATE
 * @return -ENODATA attribute not exists and flags=XATTR_REPLACE
 * @return -EPERM attribute is not set and flags=0 (probably table write permission?)
 * @param mysql handle to connection to the database
 * @param attr name of attribute to operate
 * @param inode inode of file/directory holding attribute
 * @param val value of attribute, arbitrary binary string
 * @param sz size of value in bytes
 * @param flags operation mode:
 *   0 - create attribute, replace if attribute exists
 *   XATTR_CREATE=1 - create attribute, fail if exists
 *   XATTR_REPLACE=2 - replace existing attribute, fail if not exists
 */
int query_setxattr(MYSQL *mysql, const char * attr, long inode, const char * val, size_t sz, int flags)
{
MYSQL_STMT *stmt;
MYSQL_BIND ps_params[3];
unsigned long ino_len=sizeof(inode),attr_len=strlen(attr),val_sz=sz;
uint64_t affected_rows;
my_bool is_null=0,err_ino,err_attr,err_val;
const char * query;
char sql[SQL_MAX];

switch (flags){
// https://man7.org/linux/man-pages/man2/setxattr.2.html
  case 0: query = "REPLACE into %s (value, attr,  inode) VALUES (?, ?, ?)"; break;
  case XATTR_CREATE: query ="INSERT IGNORE into %s (value, attr, inode) VALUES (?, ?, ?)"; break;
  case XATTR_REPLACE: query="UPDATE %s SET value=? WHERE attr=? AND inode=?"; break;
  default: return -EINVAL;
}
stmt = mysql_stmt_init(mysql);
if (mysql_stmt_prepare(stmt, sql, snprintf(sql,SQL_MAX, query, tables->xattr))){
    log_printf(LOG_ERROR, "mysql_stmt_prepare() failed: %s\n", mysql_stmt_error(stmt));
    return -EIO;
    }

    if (mysql_stmt_param_count(stmt) != 3) {
      log_printf(LOG_ERROR, "%s(): stmt_param_count=%d, expected 3\n", __func__, mysql_stmt_param_count(stmt));
      mysql_stmt_close(stmt);
      return -EIO;
    }

memset(ps_params, 0, sizeof(ps_params));

ps_params[2].buffer_type=MYSQL_TYPE_LONG;
ps_params[2].buffer=&inode;
//ps_params[2].buffer_length=0;
//ps_params[2].length=0;
//ps_params[2].is_null=0;
ps_params[2].is_unsigned=1;
ps_params[2].error=&err_ino;

ps_params[1].buffer_type=MYSQL_TYPE_STRING;
ps_params[1].buffer=(char*)attr;
ps_params[1].buffer_length=attr_len;
ps_params[1].length=&attr_len;
//ps_params[1].is_null=0;
ps_params[1].error=&err_attr;

ps_params[0].buffer_type=MYSQL_TYPE_BLOB;
ps_params[0].buffer=(char*)val;
ps_params[0].buffer_length=val_sz;
ps_params[0].length=&val_sz;
//ps_params[0].is_null=0;
ps_params[0].error=&err_val;

if (mysql_stmt_bind_param(stmt, ps_params)){
      log_printf(LOG_ERROR, "%s(): mysql_stmt_bind_param error: %s\n", __func__,mysql_stmt_error(stmt));
      mysql_stmt_close(stmt);
      return -EIO;
}

if (mysql_stmt_execute(stmt)){
      log_printf(LOG_ERROR, "%s(): mysql_stmt_execute error: %s\n", __func__,mysql_stmt_error(stmt));
      mysql_stmt_close(stmt);
      return -EIO;
}

affected_rows= mysql_stmt_affected_rows(stmt);

if (mysql_stmt_close(stmt)){
      log_printf(LOG_ERROR, "%s(): mysql_stmt_close error: %s\n", __func__,mysql_error(mysql));
      return -EIO;
}

if (!affected_rows){
  switch (flags){
    case XATTR_CREATE: return -EEXIST;
    case XATTR_REPLACE: return -ENODATA;
    default: return -EPERM;
  }
}
  return 0;

}
