/*
 * mtnd.h
 */
#ifndef _MTND_H
#define _MTND_H
#include "mtn.h"
#include "libmtn.h"
#include "common.h"
#include "ioprio.h"

#define MTND_EXPORT_RETURN               \
  if(!ctx->export){                      \
    kt->fin = 1;                         \
    kt->send.head.type = MTNCMD_SUCCESS; \
    kt->send.head.size = 0;              \
    kt->send.head.fin  = 1;              \
    return;                              \
  }                                      \

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
  int  rdonly;
  int  ioprio;
  int  fsig[2];
} MTND;

typedef void (*MTNFSTASKFUNC)(MTNTASK *);
extern volatile sig_atomic_t is_loop;
extern MTN  *mtn;
extern MTND *ctx;

int mtnd_fix_path(char *path, char *real);
int getstatd(uint64_t *dfree, uint64_t *dsize);
int is_freelimit(void);
void init_task_child(void);
void mtnd_child(MTNTASK *kt);
#endif
