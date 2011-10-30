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
#define MTNFS_CONTEXT ((MTNFS *)(fuse_get_context()->private_data))
#define MTN_CONTEXT   (MTNFS_CONTEXT->mtn)
#define FUSE_UID (fuse_get_context()->uid)
#define FUSE_GID (fuse_get_context()->gid)

struct cmdopt {
  char *pid;
  char *addr;
  char *port;
  char *group;
  char *loglevel;
  int   dontfork;
};

typedef struct mtnfs_context{
  MTN    *mtn;
  MTNDIR *dircache;
  char pid[PATH_MAX];
  char cwd[PATH_MAX];
  pthread_mutex_t *file_mutex;
  pthread_mutex_t cache_mutex;
} MTNFS;
