/*
 * mtnd.c
 * Copyright (C) 2011 KLab Inc.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "mtn.h"
#include "mtnd.h"
#include "libmtn.h"
#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <utime.h>
#include <fcntl.h>
#include <getopt.h>
#include <grp.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/wait.h>

MTN     *mtn;
MTND    *ctx;
MTNTASK *tasklist = NULL;
MTNSAVETASK *tasksave = NULL;
typedef void (*MTNFSTASKFUNC)(MTNTASK *);
MTNFSTASKFUNC taskfunc[2][MTNCMD_MAX];
static int is_loop = 1;

void version()
{
  printf("%s version %s\n", MODULE_NAME, MTN_VERSION);
}

void usage()
{
  printf("%s version %s\n", MODULE_NAME, MTN_VERSION);
  printf("usage: %s [OPTION]\n", MODULE_NAME);
  printf("\n");
  printf("  OPTION\n");
  printf("   -h               # help\n");
  printf("   -v               # version\n");
  printf("   -n               # no daemon\n");
  printf("   -e dir           # export dir\n");
  printf("   -E dir           # execute dir\n");
  printf("   -g name,name,... # group list(MAX 64Bytes)\n");
  printf("   -m addr          # mcast addr\n");
  printf("   -p port          # TCP/UDP port(default: 6000)\n");
  printf("   -D num           # debug level\n");
  printf("   -l size          # limit size (MB)\n");
  printf("   --pid=path       # pid file(ex: /var/run/mtnfs.pid)\n");
  printf("\n");
}

int getstatf(uint32_t *bsize, uint32_t *fsize, uint64_t *dsize, uint64_t *dfree)
{
  struct statvfs vf;
  if(statvfs(".", &vf) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  *bsize = vf.f_bsize;
  *fsize = vf.f_frsize;
  *dsize = vf.f_blocks;
  *dfree = vf.f_bfree;
  return(0);
}

int is_freelimit()
{
  uint32_t bsize;
  uint32_t fsize;
  uint64_t dsize;
  uint64_t dfree;
  getstatf(&bsize, &fsize, &dsize, &dfree);
  dfree *= bsize;
  return(ctx->free_limit > dfree);
}

int is_savetask(MTNTASK *mt)
{
  MTNSAVETASK *t;
  for(t=tasksave;t;t=t->next){
    if(!cmpaddr(&(mt->addr), &(t->addr))){
      if(mt->recv.head.sqno == t->sqno){
        return(1);
      }
    }
  }
  return(0);
}

uint64_t get_datasize(char *path)
{
  char full[PATH_MAX];
  off_t size = 0;
  DIR *d = opendir(path);
  struct dirent *ent;
  struct stat st;

  while((ent = readdir(d))){
    if(ent->d_name[0] == '.'){
      continue;
    }
    sprintf(full, "%s/%s", path, ent->d_name);
    if(lstat(full, &st) == -1){
      continue;
    }
    if(S_ISDIR(st.st_mode)){
      size += get_datasize(full);
      continue;
    }
    if(S_ISREG(st.st_mode)){
      size += st.st_size;
    }
  }
  closedir(d);
  return(size);
}

char *mtnd_fix_path(char *path){
  char buff[PATH_MAX];
  strcpy(buff, path);
  if(buff[0] == '/'){
    strcpy(path, buff + 1);
    return mtnd_fix_path(path);
  }
  if(memcmp(buff, "./", 2) == 0){
    strcpy(path, buff + 2);
    return mtnd_fix_path(path);
  }
  if(memcmp(buff, "../", 3) == 0){
    strcpy(path, buff + 3);
    return mtnd_fix_path(path);
  }
  return(path);
}

MTNTASK *mtnd_task_create(MTNDATA *data, MTNADDR *addr)
{
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  MTNTASK *kt;
  for(kt=tasklist;kt;kt=kt->next){
    if(cmpaddr(&(kt->addr), addr) != 0){
      continue;
    }
    if(kt->recv.head.sqno != data->head.sqno){
      continue;
    }
    return(NULL);
  }
  kt = newtask();
  gettimeofday(&(kt->tv),NULL);
  memcpy(&(kt->addr), addr, sizeof(MTNADDR));
  memcpy(&(kt->recv), data, sizeof(MTNDATA));
  kt->type = data->head.type;
  mtnlogger(mtn,8, "[debug] %s: CMD=%s SEQ=%d\n", __func__, mtncmdstr[kt->type], data->head.sqno);
  if(tasklist){
    tasklist->prev = kt;
  }
  kt->next  = tasklist;
  tasklist = kt;
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
  return(kt);
}

MTNTASK *mtnd_task_save(MTNTASK *t)
{
  MTNTASK *n;
  MTNSAVETASK *s;
  if(!t){
    return(NULL);
  }
  n = cuttask(t);
  if(t == tasklist){
    tasklist = n;
  }

  if(!(s = newsavetask(t))){
    /* mem alloc error */
  }else{
    if((s->next = tasksave)){
      s->next->prev = s;
    }
    tasksave = s;
  }
  deltask(t);
  return(n);
}

//-------------------------------------------------------------------
// UDP PROSESS
//-------------------------------------------------------------------
static void mtnd_startup_process(MTNTASK *kt)
{
  MTNSVR *mb;
  kt->addr.addr.in.sin_port = htons(mtn->mcast_port);
  ctx->members = addsvr(ctx->members, &(kt->addr), NULL);
  mb = getsvr(ctx->members, &(kt->addr));
  mtndata_get_svrhost(mb, &(kt->recv));
  gettimeofday(&(mb->tv),NULL);
  kt->fin = 1;
  kt->send.head.type = MTNCMD_STARTUP;
  kt->send.head.size = 0;
  kt->send.head.flag = 1;
  mtndata_set_string(ctx->host, &(kt->send));
}

static void mtnd_shutdown_process(MTNTASK *kt)
{
  MTNSVR *mb;
  kt->addr.addr.in.sin_port = htons(mtn->mcast_port);
  if((mb = getsvr(ctx->members, &(kt->addr)))){
    if(mb == ctx->members){
      mb = (ctx->members = delsvr(ctx->members));
    }else{
      mb = delsvr(mb);
    }
  }
  kt->fin = 1;
}

static void mtnd_hello_process(MTNTASK *kt)
{
  uint32_t mcount;

  kt->fin = 1;
  if(is_savetask(kt)){
    kt->recv.head.flag = 1;
    return;
  }
  if(kt->recv.head.flag == 1){
    kt->fin = 2;
  }else{
    mcount = get_members_count(ctx->members);
    mtndata_set_string(ctx->host, &(kt->send));
    mtndata_set_int(&mcount, &(kt->send), sizeof(mcount));
  }
}

static void mtnd_info_process(MTNTASK *kt)
{
  statm  sm;
  MTNSVR mb;
  double loadavg;

  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  memset(&mb, 0, sizeof(mb));
  getstatm(&sm);
  getstatf(&(mb.bsize), &(mb.fsize), &(mb.dsize), &(mb.dfree));
  getmeminfo(&(mb.memsize), &(mb.memfree));
  mb.limit   = ctx->free_limit;
  mb.cnt.prc = getpscount();
  mb.cnt.cld = ctx->cld.count;
  mb.cnt.mbr = get_members_count(ctx->members);
  mb.cnt.mem = getcount(MTNCOUNT_MALLOC);
  mb.cnt.tsk = getcount(MTNCOUNT_TASK);
  mb.cnt.tsv = getcount(MTNCOUNT_SAVE);
  mb.cnt.svr = getcount(MTNCOUNT_SVR);
  mb.cnt.dir = getcount(MTNCOUNT_DIR);
  mb.cnt.sta = getcount(MTNCOUNT_STAT);
  mb.cnt.str = getcount(MTNCOUNT_STR);
  mb.cnt.arg = getcount(MTNCOUNT_ARG);
  mb.cnt.cpu = sysconf(_SC_NPROCESSORS_ONLN);
  getloadavg(&loadavg, 1);
  mb.loadavg = (uint32_t)(loadavg * 100);
  mb.flags  |= ctx->export  ? MTNMODE_EXPORT  : 0;
  mb.flags  |= ctx->execute ? MTNMODE_EXECUTE : 0;
  mtndata_set_int(&(mb.bsize),      &(kt->send), sizeof(mb.bsize));
  mtndata_set_int(&(mb.fsize),      &(kt->send), sizeof(mb.fsize));
  mtndata_set_int(&(mb.dsize),      &(kt->send), sizeof(mb.dsize));
  mtndata_set_int(&(mb.dfree),      &(kt->send), sizeof(mb.dfree));
  mtndata_set_int(&(mb.limit),      &(kt->send), sizeof(mb.limit));
  mtndata_set_int(&(sm.vsz),        &(kt->send), sizeof(sm.vsz));
  mtndata_set_int(&(sm.res),        &(kt->send), sizeof(sm.res));
  mtndata_set_int(&(mb.cnt.cpu),    &(kt->send), sizeof(mb.cnt.cpu));
  mtndata_set_int(&(mb.loadavg),    &(kt->send), sizeof(mb.loadavg));
  mtndata_set_int(&(mb.memsize),    &(kt->send), sizeof(mb.memsize));
  mtndata_set_int(&(mb.memfree),    &(kt->send), sizeof(mb.memfree));
  mtndata_set_int(&(mb.flags),      &(kt->send), sizeof(mb.flags));
  mtndata_set_data(&(mb.cnt),       &(kt->send), sizeof(mb.cnt));
  mtndata_set_string(mtn->groupstr, &(kt->send));
  kt->send.head.fin = 1;
  kt->fin = 1;
  mtnlogger(mtn, 8, "[debug] %s: SIZE=%d\n", __func__, kt->send.head.size);
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

int mtnd_list_dir(MTNTASK *kt)
{
  uint16_t len;
  struct  dirent *ent;
  char full[PATH_MAX];
  while((ent = readdir(kt->dir))){
    if(ent->d_name[0] == '.'){
      continue;
    }
    sprintf(full, "%s/%s", kt->path, ent->d_name);
    if(!lstat(full, &(kt->stat))){
      len  = mtndata_set_string(ent->d_name, NULL);
      len += mtndata_set_stat(&(kt->stat),   NULL);
      if(kt->send.head.size + len <= mtn->max_packet_size){
        mtndata_set_string(ent->d_name, &(kt->send));
        mtndata_set_stat(&(kt->stat),   &(kt->send));
      }else{
        send_dgram(mtn, kt->con, &(kt->send), &(kt->addr));
        memset(&(kt->send), 0, sizeof(kt->send));
        mtndata_set_string(ent->d_name, &(kt->send));
        mtndata_set_stat(&(kt->stat),   &(kt->send));
        return(0);
      }
    } 
  }
  closedir(kt->dir);
  kt->dir = NULL;
  kt->fin = 1;
  kt->send.head.fin = 1;
  return(0);
}

//-------------------------------------------------------------------
// UDP EXPORT PROSESS
//-------------------------------------------------------------------
static void mtnd_list_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }

  char buff[PATH_MAX];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  if(kt->dir){
    mtnd_list_dir(kt);
  }else{
    kt->fin = 1;
    kt->send.head.size = 0;
    mtndata_get_string(buff, &(kt->recv));
    mtnd_fix_path(buff);
    sprintf(kt->path, "./%s", buff);
    if(lstat(kt->path, &(kt->stat)) == -1){
      if(errno != ENOENT){
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
      }
      kt->send.head.fin = 1;
    }
    if(S_ISREG(kt->stat.st_mode)){
      mtndata_set_string(basename(kt->path), &(kt->send));
      mtndata_set_stat(&(kt->stat), &(kt->send));
      kt->send.head.fin = 1;
    }
    if(S_ISDIR(kt->stat.st_mode)){
      if((kt->dir = opendir(kt->path))){
        kt->fin = 0;
      }else{
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUI\n", __func__);
}

static void mtnd_stat_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  char buff[PATH_MAX];
  char file[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(buff, &(kt->recv));
  mtnd_fix_path(buff);
  sprintf(kt->path, "./%s", buff);
  if(lstat(kt->path, &(kt->stat)) == -1){
    if(errno != ENOENT){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
      kt->send.head.type = MTNCMD_ERROR;
    }
  }else{
    kt->send.head.type = MTNCMD_SUCCESS;
    dirbase(kt->path, NULL, file);
    mtndata_set_string(file, &(kt->send));
    mtndata_set_stat(&(kt->stat), &(kt->send));
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_truncate_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }
  off_t offset;
  char path[PATH_MAX];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.type = MTNCMD_SUCCESS;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(path, &(kt->recv));
  mtndata_get_int(&offset, &(kt->recv), sizeof(offset));
  mtnd_fix_path(path);
  sprintf(kt->path, "./%s", path);
  if(truncate(kt->path, offset) == -1){
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: %s path=%s offset=%llu\n", __func__, strerror(errno), kt->path, offset);
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_mkdir_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  uid_t uid;
  gid_t gid;
  char buff[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(buff, &(kt->recv));
  mtndata_get_int(&uid, &(kt->recv), sizeof(uid));
  mtndata_get_int(&gid, &(kt->recv), sizeof(gid));
  mtnd_fix_path(buff);
  sprintf(kt->path, "./%s", buff);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(mkdir_ex(kt->path) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
  }
  chown(kt->path, uid, gid);
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_rm_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  char buff[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(buff, &(kt->recv));
  mtnd_fix_path(buff);
  sprintf(kt->path, "./%s", buff);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(lstat(kt->path, &(kt->stat)) != -1){
    if(S_ISDIR(kt->stat.st_mode)){
      if(rmdir(kt->path) == -1){
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        kt->send.head.type = MTNCMD_ERROR;
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }else{
      if(unlink(kt->path) == -1){
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        kt->send.head.type = MTNCMD_ERROR;
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_rename_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  char obuff[PATH_MAX];
  char nbuff[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(obuff, &(kt->recv));
  mtndata_get_string(nbuff, &(kt->recv));
  mtnd_fix_path(obuff);
  mtnd_fix_path(nbuff);
  sprintf(kt->path, "./%s", obuff);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(rename(obuff, nbuff) == -1){
    if(errno != ENOENT){
      mtnlogger(mtn, 0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), obuff, nbuff);
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_symlink_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  char oldpath[PATH_MAX];
  char newpath[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(oldpath, &(kt->recv));
  mtndata_get_string(newpath, &(kt->recv));
  mtnd_fix_path(newpath);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(symlink(oldpath, newpath) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), oldpath, newpath);
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_readlink_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  ssize_t size;
  char newpath[PATH_MAX];
  char oldpath[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(newpath, &(kt->recv));
  mtnd_fix_path(newpath);
  kt->send.head.type = MTNCMD_SUCCESS;
  size = readlink(newpath, oldpath, PATH_MAX);
  if(size == -1){
    mtnlogger(mtn, 0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), oldpath, newpath);
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
  }else{
    oldpath[size] = 0;
    mtndata_set_string(oldpath, &(kt->send));
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_chmod_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }
  mode_t mode;
  char path[PATH_MAX];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(path, &(kt->recv));
  mtndata_get_int(&mode, &(kt->recv), sizeof(mode));
  mtnd_fix_path(path);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(chmod(path, mode) == -1){
    if(errno != ENOENT){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_chown_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }
  uid_t uid;
  gid_t gid;
  char path[PATH_MAX];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(path, &(kt->recv));
  mtndata_get_int(&uid, &(kt->recv), sizeof(uid));
  mtndata_get_int(&gid, &(kt->recv), sizeof(gid));
  mtnd_fix_path(path);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(chown(path, uid, gid) == -1){
    if(errno != ENOENT){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_utime_process(MTNTASK *kt)
{
  if(!ctx->export){
    kt->fin = 1;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->send.head.fin  = 1;
    return;
  }
  struct utimbuf ut;
  char path[PATH_MAX];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(path, &(kt->recv));
  mtndata_get_int(&(ut.actime),  &(kt->recv), sizeof(ut.actime));
  mtndata_get_int(&(ut.modtime), &(kt->recv), sizeof(ut.modtime));
  mtnd_fix_path(path);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(utime(path, &ut) == -1){
    if(errno != ENOENT){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

void mtnd_udp_process(int s)
{
  MTNADDR addr;
  MTNDATA data;
  char buf[INET_ADDRSTRLEN];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  addr.len = sizeof(addr.addr);
  if(recv_dgram(mtn, s, &data, &(addr.addr.addr), &(addr.len)) != -1){
    mtnlogger(mtn, 8, "[debug] %s: from %s\n", __func__, v4addr(&addr, buf));
    mtnd_task_create(&data, &addr);
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

void mtnd_task_process(int s)
{
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  MTNTASK *t = tasklist;
  MTNFSTASKFUNC task;
  while(t){
    t->con = s;
    task = taskfunc[0][t->type];
    if(task){
      task(t);
    }else{
      t->fin = 1;
      t->send.head.fin  = 1;
      t->send.head.size = 0;
      t->send.head.type = MTNCMD_ERROR;
      errno = EPERM;
      mtndata_set_int(&errno, &(t->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: Function Not Found type=%d\n", __func__, t->type);
    }
    if(t->fin){
      if(t->recv.head.flag == 0){
        send_dgram(mtn, s, &(t->send), &(t->addr));
      }
      switch(t->fin){
        case 1:
          if(t == tasklist){
            t = tasklist = deltask(t);
          }else{
            t = deltask(t);
          }
          break;
        case 2:
          t = mtnd_task_save(t);
          break;
      }
    }else{
      t = t->next;
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

//------------------------------------------------------
// mtntool commands for TCP
//------------------------------------------------------
void mtnd_child_get(MTNTASK *kt)
{
  if(kt->fd == -1){
    errno = EBADF;
    mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }
  while(is_loop){
    kt->send.head.size = read(kt->fd, kt->send.data.data, sizeof(kt->send.data.data));
    if(kt->send.head.size == 0){ // EOF
      close(kt->fd);
      kt->send.head.fin = 1;
      kt->fd = 0;
      break;
    }
    if(kt->send.head.size == -1){
      mtnlogger(mtn, 0,"[error] %s: file read error %s %s\n", __func__, strerror(errno), kt->path);
      kt->send.head.type = MTNCMD_ERROR;
      kt->send.head.size = 0;
      kt->send.head.fin  = 1;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      break;
    }
    if(send_data_stream(mtn, kt->con, &(kt->send)) == -1){
      mtnlogger(mtn, 0,"[error] %s: send error %s %s\n", __func__, strerror(errno), kt->path);
      kt->send.head.type = MTNCMD_ERROR;
      kt->send.head.size = 0;
      kt->send.head.fin  = 1;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      break;
    }
  }
}

void mtnd_child_put(MTNTASK *kt)
{
  char d[PATH_MAX];
  char f[PATH_MAX];

  mtnlogger(mtn, 9, "[debug] %s: START\n", __func__);
  if(kt->fd){
    if(kt->recv.head.size == 0){
      //----- EOF -----
      struct timeval tv[2];
      tv[0].tv_sec  = kt->stat.st_atime;
      tv[0].tv_usec = 0;
      tv[1].tv_sec  = kt->stat.st_mtime;
      tv[1].tv_usec = 0;
      futimes(kt->fd, tv);
      close(kt->fd);
      kt->send.head.fin = 1;
      kt->fin = 1;
      kt->fd = 0;
    }else{
      //----- write -----
      kt->res = write(kt->fd, kt->recv.data.data, kt->recv.head.size);
      if(kt->res == -1){
        mtnlogger(mtn, 0, "[error] %s: file write error %s\n",__func__, strerror(errno));
        kt->fin = 1;
        kt->send.head.fin = 1;
        kt->send.head.type = MTNCMD_ERROR;
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }else{
    //----- open/create -----
    mtndata_get_string(kt->path,  &(kt->recv));
    mtndata_get_stat(&(kt->stat), &(kt->recv));
    mtnlogger(mtn, 0,"[info]  %s: creat %s\n", __func__, kt->path);
    dirbase(kt->path, d, f);
    if(mkdir_ex(d) == -1){
      mtnlogger(mtn, 0,"[error] %s: mkdir error %s %s\n", __func__, strerror(errno), d);
      kt->fin = 1;
      kt->send.head.fin = 1;
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      kt->fd = creat(kt->path, kt->stat.st_mode);
      if(kt->fd == -1){
        mtnlogger(mtn, 0,"[error] %s: can't creat %s %s\n", __func__, strerror(errno), kt->path);
        kt->fd  = 0;
        kt->fin = 1;
        kt->send.head.fin = 1;
        kt->send.head.type = MTNCMD_ERROR;
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: END\n", __func__);
}

//------------------------------------------------------
// mtnmount commands for TCP
//------------------------------------------------------
void mtnd_child_open(MTNTASK *kt)
{
  int flags;
  mode_t mode;
  char d[PATH_MAX];
  char f[PATH_MAX];
  mtndata_get_string(kt->path, &(kt->recv));
  mtndata_get_int(&flags, &(kt->recv), sizeof(flags));
  mtndata_get_int(&mode,  &(kt->recv), sizeof(mode));
  mtnd_fix_path(kt->path);
  dirbase(kt->path, d, f);
  mkdir_ex(d);
  kt->fd = open(kt->path, flags, mode);
  if(kt->fd == -1){
    kt->fd = 0;
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    mtnlogger(mtn, 0, "[error] %s: %s, path=%s create=%d mode=%o\n", __func__, strerror(errno), kt->path, ((flags & O_CREAT) != 0), mode);
  }else{
    fstat(kt->fd, &(kt->stat));
    mtnlogger(mtn, 1, "[info]  %s: path=%s create=%d mode=%o\n", __func__, kt->path, ((flags & O_CREAT) != 0), mode);
  }
}

void mtnd_child_read(MTNTASK *kt)
{
  int r;
  size_t  size;
  off_t offset;
  mtndata_get_int(&size,   &(kt->recv), sizeof(size));
  mtndata_get_int(&offset, &(kt->recv), sizeof(offset));

  size = (size > MTN_MAX_DATASIZE) ? MTN_MAX_DATASIZE : size;
  r = pread(kt->fd, kt->send.data.data, size, offset);
  kt->send.head.fin  = 1;
  kt->send.head.size = (r > 0) ? r : 0;
}

void mtnd_child_write(MTNTASK *kt)
{
  size_t  size;
  void   *buff;
  off_t offset;
  static size_t total = 0;

  if(mtndata_get_int(&offset, &(kt->recv), sizeof(offset)) == -1){
    errno = EIO;
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }
  size = kt->recv.head.size;
  buff = kt->recv.data.data;

  if(total > 1024 * 1024){
    if(is_freelimit()){
      errno = ENOSPC;
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      return;
    }
    total = 0;
  }
  while(size){
    kt->res = pwrite(kt->fd, buff, size, offset);
    if(kt->res == -1){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      break;
    }
    total  += kt->res;
    offset += kt->res;
    buff   += kt->res;
    size   -= kt->res;
  }
}

void mtnd_child_close(MTNTASK *kt)
{
  if(kt->fd){
    fstat(kt->fd, &(kt->stat));
    if(close(kt->fd) == -1){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }
    kt->fd = 0;
  }
}

void mtnd_child_truncate(MTNTASK *kt)
{
  off_t offset;
  mtndata_get_string(kt->path, &(kt->recv));
  mtndata_get_int(&offset, &(kt->recv), sizeof(offset));
  mtnd_fix_path(kt->path);
  stat(kt->path, &(kt->stat));
  if(truncate(kt->path, offset) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    mtnlogger(mtn, 0, "[error] %s: %s path=%s offset=%llu\n", __func__, strerror(errno), kt->path, offset);
  }else{
    mtnlogger(mtn, 2, "[debug] %s: path=%s offset=%llu\n", __func__, kt->path, offset);
  }
}

void mtnd_child_mkdir(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: START\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path);
  if(mkdir(kt->path, 0777) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
  }
  mtnlogger(mtn, 8, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  mtnlogger(mtn, 9, "[debug] %s: END\n", __func__);
}

void mtnd_child_rmdir(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: START\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path);
  if(rmdir(kt->path) == -1){
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  mtnlogger(mtn, 8, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  mtnlogger(mtn, 9, "[debug] %s: END\n", __func__);
}

void mtnd_child_unlink(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: START\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path);
  if(lstat(kt->path, &(kt->stat)) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    mtnlogger(mtn, 0,"[debug] %s: stat error: %s %s\n", __func__, strerror(errno), kt->path);
    return;
  }
  if(unlink(kt->path) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    mtnlogger(mtn, 0,"[debug] %s: unlink error: %s %s\n", __func__, strerror(errno), kt->path);
    return;
  }
  mtnlogger(mtn, 8, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  mtnlogger(mtn, 9, "[debug] %s: END\n", __func__);
}

void mtnd_child_getattr(MTNTASK *kt)
{
  mtnlogger(mtn, 1, "[debug] %s: START\n", __func__);
  struct stat st;
  if(kt->fd){
    if(fstat(kt->fd, &st) == -1){
      mtnlogger(mtn, 0, "[error] %s: 1\n", __func__);
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtndata_set_stat(&st, &(kt->send));
      mtnlogger(mtn, 1, "[debug] %s: 2 size=%d\n", __func__, kt->send.head.size);
    }
  }else{
    mtndata_get_string(kt->path, &(kt->recv));
    if(lstat(kt->path, &st) == -1){
      mtnlogger(mtn, 0, "[error] %s: 3\n", __func__);
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtnlogger(mtn, 1, "[debug] %s: 4\n", __func__);
      mtndata_set_stat(&st, &(kt->send));
    }
  }
  mtnlogger(mtn, 1, "[debug] %s: END\n", __func__);
}

void mtnd_child_chmod(MTNTASK *kt)
{
  uint32_t mode;
  mtnlogger(mtn, 0,"[debug] %s: START\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtndata_get_int(&mode, &(kt->recv), sizeof(mode));
  if(chmod(kt->path, mode) == -1){
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
  }else{    
    if(stat(kt->path, &(kt->stat)) == -1){
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtndata_set_stat(&(kt->stat), &(kt->send));
    }
  }
  mtnlogger(mtn, 0,"[debug] %s: END\n", __func__);
}

void mtnd_child_chown(MTNTASK *kt)
{
  uid_t uid;
  uid_t gid;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtndata_get_int(&uid, &(kt->recv), sizeof(uid));
  mtndata_get_int(&gid, &(kt->recv), sizeof(uid));
  if(chown(kt->path, uid, gid) == -1){
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
  }else{    
    if(stat(kt->path, &(kt->stat)) == -1){
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtndata_set_stat(&(kt->stat), &(kt->send));
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

void mtnd_child_setattr(MTNTASK *kt)
{
  mtnlogger(mtn, 0,"[debug] %s: START\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtnlogger(mtn, 0,"[debug] %s: END\n", __func__);
}

void mtnd_child_result(MTNTASK *kt)
{
  memcpy(&(kt->send), &(kt->keep), sizeof(MTNDATA));
  kt->keep.head.type = MTNCMD_SUCCESS;
  kt->keep.head.size = 0;
}

void mtnd_child_init_export(MTNTASK *kt)
{
  if(!ctx->export || !strlen(ctx->cwd)){
    kt->fin = 1;
    errno = EPERM;
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
  }
}

void mtnd_child_init_exec(MTNTASK *kt)
{
  if(!ctx->execute || !strlen(ctx->ewd)){
    kt->fin = 1;
    errno = EPERM;
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
  }else{
    sprintf(kt->work, "%s/%05d%05d%05d", ctx->ewd, kt->init.uid, kt->init.gid, getpid());
    if(mkdir(kt->work, 0770) == -1){
      kt->fin = 1;
      mtnlogger(mtn, 0, "[error] %s: can't mkdir %s %s\n", __func__, strerror(errno), kt->work);
    }else if(chown(kt->work, kt->init.uid, kt->init.gid) == -1){
      kt->fin = 1;
      mtnlogger(mtn, 0, "[error] %s: can't chown %s %s\n", __func__, strerror(errno), kt->work);
    }else if(chdir(kt->work) == -1){
      kt->fin = 1;
      mtnlogger(mtn, 0, "[error] %s: can't chdir %s %s\n", __func__, strerror(errno), kt->work);
    }
  }
  if(kt->fin){
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
  }
}

void mtnd_child_init(MTNTASK *kt)
{
  int r = 0;
  uid_t uid;
  gid_t gid;

  uid = getuid();
  gid = getgid();
  r += mtndata_get_int(&(kt->init.uid),  &(kt->recv), sizeof(kt->init.uid));
  r += mtndata_get_int(&(kt->init.gid),  &(kt->recv), sizeof(kt->init.gid));
  r += mtndata_get_int(&(kt->init.mode), &(kt->recv), sizeof(kt->init.mode)); 
  if(r){
    mtnlogger(mtn, 0, "[error] %s: mtn protocol error\n", __func__);
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    kt->fin = 1;
    return;
  }

  if(uid){
    if((uid != kt->init.uid) || (gid != kt->init.gid)){
      kt->fin = 1;
      errno = EPERM;
      mtnlogger(mtn, 0, "[error] %s: id mismatch uid=%d gid=%d\n", __func__, kt->init.uid, kt->init.gid);
    }
  }else{
    if(setgroups(0, NULL) == -1){
      kt->fin = 1;
      mtnlogger(mtn, 0, "[error] %s: setgroups %s\n", __func__, strerror(errno));
    }else if(setgid(kt->init.gid) == -1){
      kt->fin = 1;
      mtnlogger(mtn, 0, "[error] %s: setgid %s\n", __func__, strerror(errno));
    }else if(setuid(kt->init.uid) == -1){
      kt->fin = 1;
      mtnlogger(mtn, 0, "[error] %s: setuid %s\n", __func__, strerror(errno));
    }
  }

  switch(kt->init.mode){
    case MTNMODE_EXPORT:
      mtnd_child_init_export(kt);
      break;

    case MTNMODE_EXECUTE:
      mtnd_child_init_exec(kt);
      break;
  }

  if(kt->fin){
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }

  kt->init.init = 1;
}

void mtnd_child_exit(MTNTASK *kt)
{
  kt->fin = 1;
}

void mtnd_child_fork(MTNTASK *kt, MTNJOB *job)
{
  int pp[3][2];
  pipe(pp[0]);
  pipe(pp[1]);
  pipe(pp[2]);
  job->pid = fork();
  if(job->pid == -1){
    close(pp[0][0]);
    close(pp[0][1]);
    close(pp[1][0]);
    close(pp[1][1]);
    close(pp[2][0]);
    close(pp[2][1]);
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }
  if(job->pid > 0){
    close(pp[0][0]);
    close(pp[1][1]);
    close(pp[2][1]);
    kt->std[0] = pp[0][1];
    kt->std[1] = pp[1][0];
    kt->std[2] = pp[2][0];
    fcntl(kt->std[0], F_SETFD, FD_CLOEXEC);
    fcntl(kt->std[1], F_SETFD, FD_CLOEXEC);
    fcntl(kt->std[2], F_SETFD, FD_CLOEXEC);
    return;
  }
  //===== exec process =====
  setpgid(0,0);
  close(0);
  close(1);
  close(2);
  dup2(pp[0][0],0); // stfdin
  dup2(pp[1][1],1); // stdout
  dup2(pp[2][1],2); // stderr
  close(pp[0][0]);
  close(pp[0][1]);
  close(pp[1][0]);
  close(pp[1][1]);
  close(pp[2][0]);
  close(pp[2][1]);
  execlp("sh", "sh", "-c", job->cmd, NULL);
  mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), job->cmd);
  _exit(127);
}

int mtnd_child_exec_sche(MTNTASK *kt, MTNJOB *job)
{
  scanprocess(job, 1);
  getjobusage(job);
  if(job->cpu > job->lim){
    if(job->pstat[0].state != 'T'){
      kill(-(getpgid(job->pid)), SIGSTOP);
    }
  }else{
    if(job->pstat[0].state == 'T'){
      kill(-(getpgid(job->pid)), SIGCONT);
    }
  }
  return(getwaittime(job, 1));
}

void mtnd_child_exec_poll(MTNTASK *kt, MTNJOB *job, int wtime)
{
  int r;
  static int size  = 0;
  struct epoll_event ev;
  char buff[MTN_MAX_DATASIZE];

  r = epoll_wait(job->efd, &ev, 1, wtime);
  if(r != 1){
    return;
  }
  if(ev.data.fd == kt->con){
    kill(-(getpgid(job->pid)), SIGINT);
    return;
  }
  if(ev.data.fd == kt->std[0]){
    if(size == kt->recv.head.size){
      size = 0;
      kt->send.head.size = 0;
      kt->send.head.type = MTNCMD_STDIN;
      send_recv_stream(mtn, kt->con, &(kt->send), &(kt->recv));
      if(kt->recv.head.size == 0){
        mtnlogger(mtn, 8, "[debug] %s: STDIN:  FD=%d SIZE=%d HSIZE=%d\n", __func__, kt->std[0], size, kt->recv.head.size);
        epoll_ctl(job->efd, EPOLL_CTL_DEL, kt->std[0], NULL);
        close(kt->std[0]);
        kt->std[0] = 0;
      }else{
        mtnlogger(mtn, 9, "[debug] %s: STDIN:  FD=%d SIZE=%d HSIZE=%d\n", __func__, kt->std[0], size, kt->recv.head.size);
        r = write(kt->std[0], kt->recv.data.data + size, kt->recv.head.size - size);
        if(r == -1){
          mtnlogger(mtn, 0, "[error] %s: STDIN %s\n", __func__, strerror(errno));
        }else{
          size += r;
        }
      }
    }else{
      r = write(kt->std[0], kt->recv.data.data + size, kt->recv.head.size - size);
      if(r == -1){
        mtnlogger(mtn, 0, "[error] %s: STDIN %s\n", __func__, strerror(errno));
      }else{
        size += r;
      }
      mtnlogger(mtn, 9, "[debug] %s: STDIN: FD=%d SIZE=%d HSIZE=%d\n", __func__, kt->std[0], size, kt->recv.head.size);
    }
  }
  if(ev.data.fd == kt->std[1]){
    r = read(kt->std[1], buff, sizeof(buff));
    mtnlogger(mtn, r ? 9 : 8, "[debug] %s: STDOUT: FD=%d R=%d\n", __func__, kt->std[1],r );
    if(r > 0){
      kt->send.head.type = MTNCMD_STDOUT;
      kt->send.head.size = 0;
      mtndata_set_data(buff, &(kt->send), r);
      send_data_stream(mtn, kt->con, &(kt->send));
    }
    if(r == 0){
      epoll_ctl(job->efd, EPOLL_CTL_DEL, kt->std[1], NULL);
      close(kt->std[1]);
      kt->std[1] = 0;
    }
  }
  if(ev.data.fd == kt->std[2]){
    r = read(kt->std[2], buff, sizeof(buff));
    mtnlogger(mtn, r ? 9 : 8, "[debug] %s: STDERR: FD=%d R=%d\n", __func__, kt->std[2],r);
    if(r > 0){
      kt->send.head.type = MTNCMD_STDERR;
      kt->send.head.size = 0;
      mtndata_set_data(buff, &(kt->send), r);
      send_data_stream(mtn, kt->con, &(kt->send));
    }
    if(r == 0){
      epoll_ctl(job->efd, EPOLL_CTL_DEL, kt->std[2], NULL);
      close(kt->std[2]);
      kt->std[2] = 0;
    }
  }
}

void mtnd_child_exec(MTNTASK *kt)
{
  int  rtime;
  int  wtime;
  int status;
  MTNJOB job;
  struct timeval savetv;
  struct timeval polltv;
  struct timeval keiktv;
  struct epoll_event ev;
  char buff[MTN_MAX_DATASIZE];

  if(!kt->init.init){
    mtnlogger(mtn, 0, "[error] %s: session init error\n", __func__);
    errno = EACCES;
    kt->send.head.size = 0;
    kt->send.head.ver  = PROTOCOL_VERSION;
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }
  memset(&job, 0, sizeof(job));
  mtndata_get_string(buff, &(kt->recv));
  mtndata_get_int(&(job.lim), &(kt->recv), sizeof(job.lim));
  job.cmd = newstr(buff);
  gettimeofday(&(job.start), NULL);
  mtnlogger(mtn, 0, "[debug] %s: %s\n", __func__, job.cmd);

  mtnd_child_fork(kt, &job);
  gettimeofday(&savetv, NULL);
  job.efd = epoll_create(1);
  ev.events  = EPOLLOUT;
  ev.data.fd = kt->std[0];
  epoll_ctl(job.efd, EPOLL_CTL_ADD, ev.data.fd, &ev);
  ev.events  = EPOLLIN;
  ev.data.fd = kt->std[1];
  epoll_ctl(job.efd, EPOLL_CTL_ADD, ev.data.fd, &ev);
  ev.data.fd = kt->std[2];
  epoll_ctl(job.efd, EPOLL_CTL_ADD, ev.data.fd, &ev);
  ev.data.fd = kt->con;
  epoll_ctl(job.efd, EPOLL_CTL_ADD, ev.data.fd, &ev);
  fcntl(kt->std[0], F_SETFL , O_NONBLOCK);
  fcntl(kt->std[1], F_SETFL , O_NONBLOCK);
  fcntl(kt->std[2], F_SETFL , O_NONBLOCK);

  wtime = 200;
  kt->recv.head.size = 0;
  kt->send.head.size = 0;
  kt->send.head.ver  = PROTOCOL_VERSION;
  kt->send.head.type = MTNCMD_SUCCESS;
  send_data_stream(mtn, kt->con, &(kt->send));
  while(is_loop && (kt->std[1] || kt->std[2])){
    if(job.lim){
      gettimeofday(&polltv, NULL);
      timersub(&polltv, &savetv, &keiktv);
      rtime = keiktv.tv_sec * 1000 + keiktv.tv_usec / 1000;
      if(wtime < rtime){
        memcpy(&savetv, &polltv, sizeof(struct timeval));
        wtime = mtnd_child_exec_sche(kt, &job);
      }
    }
    mtnd_child_exec_poll(kt, &job, wtime);
  }
  kill(-(getpgid(job.pid)), SIGCONT);
  if(ctx->signal){
    kill(-(getpgid(job.pid)), ctx->signal);
  }
  while(waitpid(job.pid, &status, 0) != job.pid);
  job.exit = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
  kt->send.head.type = MTNCMD_SUCCESS;
  kt->send.head.size = 0;
  mtndata_set_int(&(job.exit), &(kt->send), sizeof(job.exit));
  mtnlogger(mtn, 0, "[debug] %s: exit_code=%d\n", __func__, job.exit);
  job_close(&job);
  if(kt->std[0]){
    close(kt->std[0]);
    kt->std[0] = 0;
  }
  mtnlogger(mtn, 0, "[debug] %s: return\n", __func__);
}

void mtnd_child_error(MTNTASK *kt)
{
  errno = EACCES;
  mtnlogger(mtn, 0, "[error] %s: TYPE=%u\n", __func__, kt->recv.head.type);
  kt->send.head.type = MTNCMD_ERROR;
  mtndata_set_int(&errno, &(kt->send), sizeof(errno));
}

void mtnd_child(MTNTASK *kt)
{
  char cmd[PATH_MAX + 16];
  char addr[INET_ADDRSTRLEN];
  v4addr(&(kt->addr), addr);
  mtnlogger(mtn, 1, "[debug] %s: accept from %s:%d sock=%d\n", __func__, addr, v4port(&(kt->addr)), kt->con);
  kt->keep.head.ver  = PROTOCOL_VERSION;
  kt->keep.head.type = MTNCMD_SUCCESS;
  kt->keep.head.size = 0;
  while(is_loop && !kt->fin){
    kt->send.head.ver  = PROTOCOL_VERSION;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->res = recv_data_stream(mtn, kt->con, &(kt->recv));
    if(kt->res == -1){
      mtnlogger(mtn, 0, "[error] %s: recv error type=%d sock=%d\n", __func__, kt->recv.head.type, kt->con);
      break;
    }
    if(kt->res == 1){
      break;
    }
    if(!kt->init.init && (kt->recv.head.type != MTNCMD_INIT)){
      mtnlogger(mtn, 0, "[error] %s: not init: TYPE=%d\n", __func__, kt->recv.head.type);
      break;
    }
    if(taskfunc[1][kt->recv.head.type]){
      taskfunc[1][kt->recv.head.type](kt);
    }else{
      mtnd_child_error(kt);
    }
    if(kt->recv.head.flag == 0){
      send_data_stream(mtn, kt->con, &(kt->send));
    }else{
      if(kt->send.head.type != MTNCMD_SUCCESS){
        memcpy(&(kt->keep), &(kt->send), sizeof(MTNDATA));
      }
    }
  }
  if(kt->init.init){
    switch(kt->init.mode){
      case MTNMODE_EXPORT:
        break;
      case MTNMODE_EXECUTE:
        if(chdir(ctx->ewd) != -1){
          sprintf(cmd, "rm -fr %s", kt->work);
          system(cmd);
        }
        break;   
    }
  }
  mtnlogger(mtn, 1, "[debug] %s: close\n", __func__);
}

void mtnd_accept_process(int l)
{
  MTNTASK kt;

  memset(&kt, 0, sizeof(kt));
  kt.addr.len = sizeof(kt.addr.addr);
  kt.con = accept(l, &(kt.addr.addr.addr), &(kt.addr.len));
  if(kt.con == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }

  kt.pid = fork();
  if(kt.pid == -1){
    mtnlogger(mtn, 0, "[error] %s: fork error %s\n", __func__, strerror(errno));
    close(kt.con);
    return;
  }
  if(kt.pid){
    ctx->cld.count++;
    ctx->cld.pid = realloc(ctx->cld.pid, sizeof(pid_t) * ctx->cld.count);
    ctx->cld.pid[ctx->cld.count - 1] = kt.pid;
    close(kt.con);
    return;
  }

  //----- child process -----
  close(l);
  fcntl(kt.con, F_SETFD, FD_CLOEXEC);
  mtnd_child(&kt);
  close(kt.con);
  _exit(0);
}

/*
 * mode
 *   0: チェックしてすぐ抜ける
 *   1: 全プロセスが終了するまで待つ
 */
void mtnd_waitpid(int mode)
{
  int i;
  int opt;
  pid_t pid;

  opt = mode ? 0 : WNOHANG;
  if(mode && ctx->signal){
    for(i=0;i<ctx->cld.count;i++){
      mtnlogger(mtn, 0, "[info] %s: send signal pid=%d sig=%d\n", __func__, ctx->cld.pid[i], ctx->signal);
      kill(ctx->cld.pid[i], ctx->signal);
    }
  }
  while(ctx->cld.count){
    pid = waitpid(-1, NULL, opt);
    if(pid <= 0){
      if(mode){
        continue;
      }else{
        break;
      }
    }
    for(i=0;i<ctx->cld.count;i++){
      if(ctx->cld.pid[i] == pid){
        break;
      }
    }
    if(i == ctx->cld.count){
      if(mode){
        continue;
      }else{
        break;
      }
    }
    ctx->cld.count--;
    if(i < ctx->cld.count){
      memmove(&(ctx->cld.pid[i]), &(ctx->cld.pid[i+1]), (ctx->cld.count - i) * sizeof(pid_t));
    }
    if(ctx->cld.count){
      ctx->cld.pid = realloc(ctx->cld.pid, sizeof(pid_t) * ctx->cld.count);
    }else{
      free(ctx->cld.pid);
      ctx->cld.pid = NULL;
    }
  }
}

void mtnd_loop_startup(struct timeval *tv)
{
  static struct timeval tv_health = {0, 0};
  if((tv->tv_sec - tv_health.tv_sec) > 60){
    mtn_startup(mtn, 1);
    memcpy(&tv_health, tv, sizeof(tv_health));
  }
}

void mtnd_loop_clrtask(struct timeval *tv)
{
  MTNSAVETASK *st = tasksave;
  while(st){
    if((tv->tv_sec - st->tv.tv_sec) > 15){
      if(st == tasksave){
        tasksave = st = delsavetask(st);
      }else{
        st = delsavetask(st);
      }
      continue;
    }
    st = st->next;
  }
}

void mtnd_loop_downsvr(struct timeval *tv)
{
  MTNSVR *m = ctx->members;
  while(m){
    if((tv->tv_sec - m->tv.tv_sec) > 300){
      if(m == ctx->members){
        m = ctx->members = delsvr(m);
      }else{
        m = delsvr(m);
      }
      continue;
    }
    m = m->next;
  }
}

void mtnd_loop(int e, int l)
{
  int r;
  struct timeval tv;
  struct epoll_event ev[2];

  //===== Main Loop =====
  while(is_loop){
    mtnd_waitpid(0);
    r = epoll_wait(e, ev, 2, 1000);
    gettimeofday(&tv,NULL);
    mtnd_loop_startup(&tv);
    mtnd_loop_clrtask(&tv);
    mtnd_loop_downsvr(&tv);
    if(r == -1){
      if(errno != EINTR){
        mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
      }
      continue;
    }
    while(r--){
      if(ev[r].data.fd == l){
        mtnd_accept_process(ev[r].data.fd);
      }else{
        if(ev[r].events & EPOLLIN){
          mtnd_udp_process(ev[r].data.fd);
        }
        if(ev[r].events & EPOLLOUT){
          mtnd_task_process(ev[r].data.fd);
        }
        ev[r].events = tasklist ? EPOLLIN | EPOLLOUT : EPOLLIN;
        epoll_ctl(e, EPOLL_CTL_MOD, ev[r].data.fd, &ev[r]);
      }
    }
  }
}

int mtnd_main()
{
  int m;
  int l;
  int e;
  struct epoll_event ev;

  e = epoll_create(1);
  if(e == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  fcntl(e, F_SETFD, FD_CLOEXEC);

  m = create_msocket(mtn);
  if(m == -1){
    mtnlogger(mtn, 0, "[error] %s: can't socket create\n", __func__);
    return(-1);
  }

  l = create_lsocket(mtn);
  if(l == -1){
    mtnlogger(mtn, 0, "[error] %s: can't socket create\n", __func__);
    return(-1);
  }

  if(listen(l, 64) == -1){
    mtnlogger(mtn, 0, "%s: listen error\n", __func__);
    return(-1);
  }

  ev.data.fd = l;
  ev.events  = EPOLLIN;
  if(epoll_ctl(e, EPOLL_CTL_ADD, l, &ev) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }

  ev.data.fd = m;
  ev.events  = EPOLLIN;
  if(epoll_ctl(e, EPOLL_CTL_ADD, m, &ev) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  mtn_startup(mtn, 0);
  mtnd_loop(e, l);
  mtn_shutdown(mtn);
  mtnd_waitpid(1);
  close(e);
  close(m);
  close(l);
  return(0);
}

struct option *get_optlist()
{
  static struct option opt[]={
      {"pid",     1, NULL, 'P'},
      {"help",    0, NULL, 'h'},
      {"version", 0, NULL, 'v'},
      {"export",  1, NULL, 'e'},
      {"execute", 1, NULL, 'E'},
      {"group",   1, NULL, 'g'},
      {0, 0, 0, 0}
    };
  return(opt);
}

void init_task()
{
  memset(taskfunc, 0, sizeof(taskfunc));
  taskfunc[0][MTNCMD_STARTUP]  = (MTNFSTASKFUNC)mtnd_startup_process;
  taskfunc[0][MTNCMD_SHUTDOWN] = (MTNFSTASKFUNC)mtnd_shutdown_process;
  taskfunc[0][MTNCMD_HELLO]    = (MTNFSTASKFUNC)mtnd_hello_process;
  taskfunc[0][MTNCMD_INFO]     = (MTNFSTASKFUNC)mtnd_info_process;
  taskfunc[0][MTNCMD_LIST]     = (MTNFSTASKFUNC)mtnd_list_process;
  taskfunc[0][MTNCMD_STAT]     = (MTNFSTASKFUNC)mtnd_stat_process;
  taskfunc[0][MTNCMD_TRUNCATE] = (MTNFSTASKFUNC)mtnd_truncate_process;
  taskfunc[0][MTNCMD_MKDIR]    = (MTNFSTASKFUNC)mtnd_mkdir_process;
  taskfunc[0][MTNCMD_RMDIR]    = (MTNFSTASKFUNC)mtnd_rm_process;
  taskfunc[0][MTNCMD_UNLINK]   = (MTNFSTASKFUNC)mtnd_rm_process;
  taskfunc[0][MTNCMD_RENAME]   = (MTNFSTASKFUNC)mtnd_rename_process;
  taskfunc[0][MTNCMD_SYMLINK]  = (MTNFSTASKFUNC)mtnd_symlink_process;
  taskfunc[0][MTNCMD_READLINK] = (MTNFSTASKFUNC)mtnd_readlink_process;
  taskfunc[0][MTNCMD_CHMOD]    = (MTNFSTASKFUNC)mtnd_chmod_process;
  taskfunc[0][MTNCMD_CHOWN]    = (MTNFSTASKFUNC)mtnd_chown_process;
  taskfunc[0][MTNCMD_UTIME]    = (MTNFSTASKFUNC)mtnd_utime_process;

  taskfunc[1][MTNCMD_PUT]      = mtnd_child_put;
  taskfunc[1][MTNCMD_GET]      = mtnd_child_get;
  taskfunc[1][MTNCMD_OPEN]     = mtnd_child_open;
  taskfunc[1][MTNCMD_READ]     = mtnd_child_read;
  taskfunc[1][MTNCMD_WRITE]    = mtnd_child_write;
  taskfunc[1][MTNCMD_CLOSE]    = mtnd_child_close;
  taskfunc[1][MTNCMD_TRUNCATE] = mtnd_child_truncate;
  taskfunc[1][MTNCMD_MKDIR]    = mtnd_child_mkdir;
  taskfunc[1][MTNCMD_RMDIR]    = mtnd_child_rmdir;
  taskfunc[1][MTNCMD_UNLINK]   = mtnd_child_unlink;
  taskfunc[1][MTNCMD_CHMOD]    = mtnd_child_chmod;
  taskfunc[1][MTNCMD_CHOWN]    = mtnd_child_chown;
  taskfunc[1][MTNCMD_GETATTR]  = mtnd_child_getattr;
  taskfunc[1][MTNCMD_SETATTR]  = mtnd_child_setattr;
  taskfunc[1][MTNCMD_RESULT]   = mtnd_child_result;
  taskfunc[1][MTNCMD_INIT]     = mtnd_child_init;
  taskfunc[1][MTNCMD_EXIT]     = mtnd_child_exit;
  taskfunc[1][MTNCMD_EXEC]     = mtnd_child_exec;
}

void mtnd_startmsg()
{
  uint32_t bsize;
  uint32_t fsize;
  uint64_t dsize;
  uint64_t dfree;
  getstatf(&bsize, &fsize, &dsize, &dfree);
  mtnlogger(mtn, 0, "======= %s start =======\n", MODULE_NAME);
  mtnlogger(mtn, 0, "ver  : %s\n", MTN_VERSION);
  mtnlogger(mtn, 0, "pid  : %d\n", getpid());
  mtnlogger(mtn, 0, "log  : %d\n", mtn->loglevel);
  mtnlogger(mtn, 0, "addr : %s\n", mtn->mcast_addr);
  mtnlogger(mtn, 0, "port : %d\n", mtn->mcast_port);
  mtnlogger(mtn, 0, "host : %s\n", ctx->host);
  if(mtn->groupstr){
  mtnlogger(mtn, 0, "group: %s\n", mtn->groupstr);
  }
  if(ctx->execute){
  mtnlogger(mtn, 0, "exec : %s\n", ctx->ewd);
  }
  if(ctx->export){
  mtnlogger(mtn, 0, "base : %s\n", ctx->cwd);
  mtnlogger(mtn, 0, "size : %6llu [MB]\n", fsize * dsize / 1024 / 1024);
  mtnlogger(mtn, 0, "free : %6llu [MB]\n", bsize * dfree / 1024 / 1024);
  mtnlogger(mtn, 0, "limit: %6llu [MB]\n", ctx->free_limit/1024 / 1024);
  }
}

void mtnd_endmsg()
{
  mtnlogger(mtn, 0, "%s finished\n", MODULE_NAME);
}

int mtnd()
{
  int r = 0;
  mtnd_startmsg();
  if(mkpidfile(ctx->pid) == -1){
    r = 1;
  }else{
    if(mtnd_main() == -1){
      r = 1;
    }
    rmpidfile(ctx->pid);
  }
  mtnd_endmsg();
  return(r);
}

void signal_handler(int n)
{
  switch(n){
    case SIGINT:
    case SIGTERM:
      is_loop = 0;
      mtn_break();
      ctx->signal = n;
      break;
    case SIGPIPE:
      break;
    case SIGUSR1:
      mtn->loglevel++;
      break;
    case SIGUSR2:
      mtn->loglevel--;
      break;
  }
}

void daemonize()
{
  int pid;
  if(!ctx->daemonize){
    return;
  }
  pid = fork();
  if(pid == -1){
    mtnlogger(mtn, 0, "[error] %s: fork1: %s\n", __func__, strerror(errno));
    exit(1); 
  }
  if(pid){
    _exit(0);
  }
  setsid();
  pid = fork();
  if(pid == -1){
    mtnlogger(mtn, 0, "[error] %s: fork2: %s\n", __func__, strerror(errno));
    exit(1); 
  }
  if(pid){
    _exit(0);
  }
  //----- daemon process -----
  mtn->logmode = MTNLOG_SYSLOG;
  close(2);
  close(1);
  close(0);
  open("/dev/null",O_RDWR); // new stdin
  dup(0);                   // new stdout
  dup(0);                   // new stderr
}

void parse(int argc, char *argv[])
{
  int r;
  if(argc < 2){
    usage();
    exit(0);
  }
  while((r = getopt_long(argc, argv, "hvng:E:e:l:m:p:P:D:", get_optlist(), NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'v':
        version();
        exit(0);

      case 'D':
        mtn->loglevel = atoi(optarg);
        break;

      case 'n':
        ctx->daemonize = 0;
        break;

      case 'g':
        if(strlen(optarg) > 63){
          mtnlogger(mtn, 0, "[error] group too long. max 64bytes\n");
          exit(1);
        }
        mtn->groupstr = newstr(optarg);
        mtn->grouparg = splitstr(optarg, ",");
        break;

      case 'E':
        ctx->execute = 1;
        strcpy(ctx->ewd, optarg);
        break;

      case 'e':
        ctx->export = 1;
        if(chdir(optarg) == -1){
          mtnlogger(mtn, 0,"error: %s %s\n", strerror(errno), optarg);
          exit(1);
        }
        break;

      case 'l':
        ctx->free_limit = atoikmg(optarg);
        break;

      case 'm':
        strcpy(mtn->mcast_addr, optarg);
        break;

      case 'p':
        mtn->mcast_port = atoi(optarg);
        break;

      case 'P':
        if(*optarg == '/'){
          strcpy(ctx->pid, optarg);
        }else{
          sprintf(ctx->pid, "%s/%s", ctx->cwd, optarg);
        }
        break;

      case '?':
        usage();
        exit(1);
    }
  }
  getcwd(ctx->cwd, sizeof(ctx->cwd));
}

void set_sig_handler()
{
  struct sigaction sig;
  memset(&sig, 0, sizeof(sig));
  sig.sa_handler = signal_handler;
  if(sigaction(SIGINT,  &sig, NULL) == -1){
    mtnlogger(NULL, 0, "%s: sigaction error SIGINT\n", __func__);
    exit(1);
  }
  if(sigaction(SIGTERM, &sig, NULL) == -1){
    mtnlogger(NULL, 0, "%s: sigaction error SIGTERM\n", __func__);
    exit(1);
  }
  if(sigaction(SIGPIPE, &sig, NULL) == -1){
    mtnlogger(NULL, 0, "%s: sigaction error SIGPIPE\n", __func__);
    exit(1);
  }
  if(sigaction(SIGUSR1, &sig, NULL) == -1){
    mtnlogger(NULL, 0, "%s: sigaction error SIGUSR1\n", __func__);
    exit(1);
  }
  if(sigaction(SIGUSR2, &sig, NULL) == -1){
    mtnlogger(NULL, 0, "%s: sigaction error SIGUSR2\n", __func__);
    exit(1);
  }
}

void mtnd_init()
{
  if(!(mtn = mtn_init(MODULE_NAME))){
    exit(1);
  }
  mtn->logtype = 1;
  mtn->logmode = MTNLOG_STDERR;
  ctx = calloc(1,sizeof(MTND));
  if(!ctx){
    mtnlogger(mtn, 0, "[error] %s: calloc: %s\n", __func__, strerror(errno));
    exit(1);
  }
  if(!getcwd(ctx->cwd, sizeof(ctx->cwd))){
    mtnlogger(mtn, 0, "[error] %s: getcwd: %s\n", __func__, strerror(errno));
    exit(1);
  } 
  if(gethostname(ctx->host, sizeof(ctx->host)) == -1){
    mtnlogger(mtn, 0, "[error] %s: getcwd: %s\n", __func__, strerror(errno));
    exit(1);
  }
  ctx->daemonize = 1;
}

int main(int argc, char *argv[])
{
  mtnd_init();
  set_sig_handler();
  parse(argc, argv);
  daemonize();
  init_task();
  return(mtnd());
}

