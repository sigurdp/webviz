FROM redis:8.4.1

# For the redis user: uid=999, gid=999
USER redis

#CMD ["redis-server", "--logfile", "", "--loglevel", "debug"]