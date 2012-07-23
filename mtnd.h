/*
 * mtnd.h
 */
#ifndef _MTND_H
#define _MTND_H
#include "mtn.h"
#include "libmtn.h"
#include "common.h"

typedef struct mtnd_context{
  MTNTASK *cldtask;
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
} MTND;

typedef void (*MTNFSTASKFUNC)(MTNTASK *);
extern int is_loop;
extern MTN  *mtn;
extern MTND *ctx;

int mtnd_fix_path(char *path, char *real);
int getstatd(uint64_t *dfree, uint64_t *dsize);
int is_freelimit(void);
void init_task_child(void);
void mtnd_child(MTNTASK *kt);
#endif
