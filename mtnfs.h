/*
 * mtnfs.h
 * Copyright (C) 2011 KLab Inc.
 */
#include <mtn.h>
#include "common.h"
#define FUSE_USE_VERSION 28
#define MTNFS_OPENLIMIT  1024
#define MTNFS_STATUSNAME ".mtnstatus"
#define MTNFS_STATUSPATH "/.mtnstatus"

struct cmdopt {
  char *pid;
  char *addr;
  char *port;
  char *loglevel;
  int   dontfork;
};

typedef struct mtnfs_context{
  MTN    *mtn;
  MTNDIR *dircache;
  struct cmdopt opt;
  char pid[PATH_MAX];
  char cwd[PATH_MAX];
  pthread_mutex_t *file_mutex;
  pthread_mutex_t cache_mutex;
} MTNFS;
