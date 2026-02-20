FROM redis:8.4.1

# For the redis user: uid=999, gid=999
USER 999

#CMD ["redis-server", "--logfile", "", "--loglevel", "debug"]