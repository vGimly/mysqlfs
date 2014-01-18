# $Id: Makefile.am,v 1.4 2006/10/02 22:34:03 ludvigm Exp $

bin_PROGRAMS = mysqlfs
schema_DATA = sql/update/initial_schema.sql 
schemadir = $(datadir)/$(distdir)/sql

# because the source is not in a subdir, we cannot just put the tests in a SUBDIRS= :(
check-recursive : mysqlfs

EXTRA_DIST = $(schema_DATA) mysqlfs.spec

SUBDIRS = tests-autotest

mysqlfs_SOURCES = mysqlfs.c query.c pool.c log.c

noinst_HEADERS = mysqlfs.h query.h pool.h log.h

if DO_RPMBUILD
release = $(rpm --query --queryformat="%{RELEASE}\n" --specfile @PACKAGE@.spec |head -1)
rpm: dist-bzip2 @PACKAGE@.spec
	$(RPMBUILD) -ta $(distdir).tar.bz2
endif

