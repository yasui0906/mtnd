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
  int  signal;
  int  export;
  int  execute;
  int  fsig[2];
  struct {
    int  count;
    pid_t *pid;
  } cld;
} MTND;
