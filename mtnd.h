/*
 * mtnd.h
 */
typedef struct mtnd_context{
  MTNSVR  *members;
  int      daemonize;
  uint64_t free_limit;
  char host[HOST_NAME_MAX];
  char pid[PATH_MAX];
  char cwd[PATH_MAX];
  char ewd[PATH_MAX];
} MTND;
