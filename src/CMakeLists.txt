
add_executable(mysqlfs mysqlfs.c query.c pool.c log.c)
target_link_libraries(mysqlfs ${FUSE_LIBRARIES} ${MYSQL_LIBRARIES})
INSTALL(TARGETS mysqlfs DESTINATION bin)

