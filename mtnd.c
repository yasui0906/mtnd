//
// mtnd.c
// Copyright (C) 2011 KLab Inc.
//
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
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
#include "mtnd.h"

MTN  *mtn = NULL;
MTND *ctx = NULL;
MTNTASK *tasklist = NULL;
MTNSAVETASK *tasksave = NULL;
static MTNFSTASKFUNC taskfunc[MTNCMD_MAX];
volatile sig_atomic_t is_loop = 1;

void version()
{
  printf("%s version %s\n", MODULE_NAME, PACKAGE_VERSION);
}

void usage()
{
  printf("%s version %s\n", MODULE_NAME, PACKAGE_VERSION);
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
  printf("   --rdonly         # read only\n");
  printf("   --low-priority   # low IO priority\n");
  printf("\n");
}

int mtnd_fix_path(char *path, char *real)
{
  ARG   a = NULL;
  STR   s = NULL;
  char *p = NULL;
  char *r = NULL;
  char buff[PATH_MAX];

  strcpy(buff, path);
  p = strtok_r(buff, "/", &r);
  while(p){
    if(!strcmp("..", p)){
      s = poparg(a);
      s = clrstr(s);
    }else if(strcmp(".", p)){
      a = addarg(a, p);
    }
    p = strtok_r(NULL, "/", &r);
  }
  if(!real){
    real = path;
  }
  strcpy(real, "./");
  if((s = joinarg(a, "/"))){
    strcat(real, s);
  }
  clrarg(a);
  clrstr(s);
  return(0);
}

int getstatd(uint64_t *dfree, uint64_t *dsize)
{
  uint64_t size;
  struct statvfs vf;
  if(statvfs(".", &vf) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(dfree){  
    size  = vf.f_bfree;
    size *= vf.f_bsize;
    *dfree = (size > ctx->free_limit) ? size - ctx->free_limit : 0;
  }
  if(dsize){
    size  = vf.f_blocks;
    size *= vf.f_frsize;
    *dsize = (size > ctx->free_limit) ? size - ctx->free_limit : 0;
  }
  return(0);
}

int is_freelimit()
{
  uint64_t dfree;
  if(getstatd(&dfree, NULL) == -1){
    return(-1);
  }
  return(dfree == 0);
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
    if(strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0){
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

MTNTASK *mtnd_task_create(MTNDATA *data, MTNADDR *addr)
{
  MTNTASK *kt;

  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
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
  kt->next = tasklist;
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
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
  }else{
    if((s->next = tasksave)){
      s->next->prev = s;
    }
    tasksave = s;
  }
  deltask(t);
  return(n);
}

static int mtnd_guid_set(uid_t uid, gid_t gid)
{
  seteuid(uid);
  setegid(gid);
  return(0);
}

static int mtnd_guid_restore()
{
  uid_t uid = getuid();
  gid_t gid = getgid();
  if(uid != geteuid()){
    seteuid(uid);
  }
  if(gid != getegid()){
    setegid(gid);
  }
  return(0);
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
  statm   sm;
  MTNSVR  mb;
  MTNTASK *t;
  double loadavg;
  struct stat st;

  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  memset(&mb, 0, sizeof(mb));
  getstatm(&sm);
  getstatd(&(mb.dfree), &(mb.dsize));
  getmeminfo(&(mb.memsize), &(mb.memfree));
  for(t=ctx->cldtask;t;t=t->next){
    if(t->init.use){
      if(stat(t->path, &st) == -1){
        mb.dfree -= t->init.use;
      }else{
        if(t->init.use > st.st_size){
          mb.dfree -= (t->init.use - st.st_size);
        }
      }
    }
  }
  if(ctx->rdonly){
    mb.dfree = 0;
  }
  mb.cnt.prc = getpscount();
  mb.cnt.cld = get_task_count(ctx->cldtask);
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
  mb.flags  |= ctx->rdonly  ? MTNMODE_RDONLY  : 0;
  mtndata_set_int(&(mb.dsize),      &(kt->send), sizeof(mb.dsize));
  mtndata_set_int(&(mb.dfree),      &(kt->send), sizeof(mb.dfree));
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
    if(strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0){
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
  char buff[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  if(kt->dir){
    mtnd_list_dir(kt);
  }else{
    kt->fin = 1;
    kt->send.head.size = 0;
    mtndata_get_string(buff, &(kt->recv));
    mtnd_fix_path(buff, kt->path);
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
  char buff[PATH_MAX];
  char file[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(buff, &(kt->recv));
  mtnd_fix_path(buff, kt->path);
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
  off_t offset;
  char path[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.type = MTNCMD_SUCCESS;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(path, &(kt->recv));
  mtndata_get_int(&offset, &(kt->recv), sizeof(offset));
  mtnd_fix_path(path, kt->path);
  if(ctx->rdonly){
    if(lstat(kt->path, &(kt->stat)) != -1){
      errno = EROFS;
    }
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: %s path=%s offset=%llu\n", __func__, strerror(errno), kt->path, offset);
    }
  }else{
    if(truncate(kt->path, offset) == -1){
      if(errno != ENOENT){
        kt->send.head.type = MTNCMD_ERROR;
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
        mtnlogger(mtn, 0, "[error] %s: %s path=%s offset=%llu\n", __func__, strerror(errno), kt->path, offset);
      }
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_mkdir_process(MTNTASK *kt)
{
  uid_t uid;
  gid_t gid;
  char buff[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(buff, &(kt->recv));
  mtndata_get_int(&uid, &(kt->recv), sizeof(uid));
  mtndata_get_int(&gid, &(kt->recv), sizeof(gid));
  mtnd_fix_path(buff, kt->path);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(ctx->rdonly){
    errno = EROFS;
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
  }else{
    if(mkdir_ex(kt->path) == -1){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    }
    chown(kt->path, uid, gid);
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_rm_process(MTNTASK *kt)
{
  char buff[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(buff, &(kt->recv));
  mtnd_fix_path(buff, kt->path);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(ctx->rdonly){
    if(lstat(kt->path, &(kt->stat)) != -1){
      errno = EROFS;
    }
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    }
  }else{
    if(lstat(kt->path, &(kt->stat)) == -1){
      if(errno != ENOENT){
        kt->send.head.type = MTNCMD_ERROR;
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
      }
    }else{
      if(S_ISDIR(kt->stat.st_mode)){
        if(rmdir(kt->path) == -1){
          kt->send.head.type = MTNCMD_ERROR;
          mtndata_set_int(&errno, &(kt->send), sizeof(errno));
          mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        }
      }else{
        if(unlink(kt->path) == -1){
          kt->send.head.type = MTNCMD_ERROR;
          mtndata_set_int(&errno, &(kt->send), sizeof(errno));
          mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        }
      }
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_rename_process(MTNTASK *kt)
{
  char obuff[PATH_MAX];
  char nbuff[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(obuff, &(kt->recv));
  mtndata_get_string(nbuff, &(kt->recv));
  mtnd_fix_path(obuff, kt->path);
  mtnd_fix_path(nbuff, NULL);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(ctx->rdonly){
    if(lstat(kt->path, &(kt->stat)) != -1){
      errno = EROFS;
    }
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), obuff, nbuff);
    }
  }else{
    if(rename(kt->path, nbuff) == -1){
      if(errno != ENOENT){
        kt->send.head.type = MTNCMD_ERROR;
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
        mtnlogger(mtn, 0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), obuff, nbuff);
      }
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_symlink_process(MTNTASK *kt)
{
  char oldpath[PATH_MAX];
  char newpath[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(oldpath, &(kt->recv));
  mtndata_get_string(newpath, &(kt->recv));
  mtnd_fix_path(newpath, NULL);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(ctx->rdonly){
    errno = EROFS;
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    mtnlogger(mtn, 0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), oldpath, newpath);
  }else{
    if(symlink(oldpath, newpath) == -1){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), oldpath, newpath);
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_readlink_process(MTNTASK *kt)
{
  ssize_t size;
  char newpath[PATH_MAX];
  char oldpath[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(newpath, &(kt->recv));
  mtnd_fix_path(newpath, NULL);
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
  mode_t mode;
  char path[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 7, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(path, &(kt->recv));
  mtndata_get_int(&mode, &(kt->recv), sizeof(mode));
  mtnd_fix_path(path, NULL);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(ctx->rdonly){
    if(lstat(kt->path, &(kt->stat)) != -1){
      errno = EROFS;
    }
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
    }
  }else{
    if(chmod(path, mode) == -1){
      if(errno != ENOENT){
        kt->send.head.type = MTNCMD_ERROR;
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      }
    }
  }
  mtnlogger(mtn, 7, "[debug] %s: OUT\n", __func__);
}

static void mtnd_chown_process(MTNTASK *kt)
{
  uid_t uid;
  gid_t gid;
  char path[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 7, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(path, &(kt->recv));
  mtndata_get_int(&uid, &(kt->recv), sizeof(uid));
  mtndata_get_int(&gid, &(kt->recv), sizeof(gid));
  mtnd_fix_path(path, NULL);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(ctx->rdonly){
    if(lstat(kt->path, &(kt->stat)) != -1){
      errno = EROFS;
    }
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
    }
  }else{
    if(chown(path, uid, gid) == -1){
      if(errno != ENOENT){
        kt->send.head.type = MTNCMD_ERROR;
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      }
    }
  }
  mtnlogger(mtn, 7, "[debug] %s: OUT\n", __func__);
}

static void mtnd_utime_process(MTNTASK *kt)
{
  struct utimbuf ut;
  char path[PATH_MAX];
  MTND_EXPORT_RETURN;
  mtnlogger(mtn, 7, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtndata_get_string(path, &(kt->recv));
  mtndata_get_int(&(ut.actime),  &(kt->recv), sizeof(ut.actime));
  mtndata_get_int(&(ut.modtime), &(kt->recv), sizeof(ut.modtime));
  mtnd_fix_path(path, NULL);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(ctx->rdonly){
    if(lstat(kt->path, &(kt->stat)) != -1){
      errno = EROFS;
    }
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
    }
  }else{
    if(utime(path, &ut) == -1){
      if(errno != ENOENT){
        kt->send.head.type = MTNCMD_ERROR;
        mtndata_set_int(&errno, &(kt->send), sizeof(errno));
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      }
    }
  }
  mtnlogger(mtn, 7, "[debug] %s: OUT\n", __func__);
}

static ssize_t recv_stream_nb(MTN *mtn, int s, void *buff, size_t size)
{
  ssize_t r;
  size_t done = 0;
  while(done < size){
    if(!is_loop){
      return(-1);
    }
    r = read(s, buff, size);
    if(r == -1){
      if(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR){
        break;
      }
      mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
      return(-1);
    }
    if(r == 0){
      break;
    }
    buff += r;
    done += r;
  }
  return(ssize_t)done;
}

static ssize_t recv_data_stream_nb(MTN *mtn, int s, MTNDATA *kd, size_t nrecv)
{
  ssize_t r;
  if(nrecv < sizeof(kd->head)){
    r = recv_stream_nb(mtn, s, ((char *)&kd->head) + nrecv, sizeof(kd->head) - nrecv);
    if(r > 0 && nrecv + r >= sizeof(kd->head)){
      if(kd->head.size > MTN_MAX_DATASIZE){
        mtnlogger(mtn, 0, "[error] %s: data length too long size=%d\n", __func__, kd->head.size);
        return(-1);
      } 
    }
    return(r);
  }
  nrecv -= sizeof(kd->head);
  return(recv_stream_nb(mtn, s, ((char *)&kd->data) + nrecv, kd->head.size - nrecv));
}

int mtnd_cld_process(int s)
{
  ssize_t r;
  MTNTASK *n;
  MTNDATA *data;
  for(n=ctx->cldtask;n;n=n->next){
    if(s == n->rpp){
      data = &(n->recv);
      r = recv_data_stream_nb(mtn, s, data, n->nrecv);
      if(r == -1){
        mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
        return(-1);
      }
      n->nrecv += r;
      if(n->nrecv >= sizeof(data->head) && n->nrecv == data->head.size + sizeof(data->head)){
        switch(data->head.type){
          case MTNCMD_OPEN:
            mtndata_get_int(&(n->init.use), data, sizeof(n->init.use));
            mtndata_get_string(n->path, data);
            break;
          case MTNCMD_RDONLY:
            mtndata_get_int(&(ctx->rdonly), data, sizeof(ctx->rdonly));
            break;
        }
        memset(data, 0, sizeof(MTNDATA));
        n->nrecv = 0;
      }
      return(1);
    }
  }
  return(0);
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
    task = taskfunc[t->type];
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

MTNTASK *mtnd_accept_process(int l)
{
  int pp[2][2];
  MTNTASK *kt  = newtask();
  kt->addr.len = sizeof(kt->addr.addr);
  kt->con = accept(l, &(kt->addr.addr.addr), &(kt->addr.len));

  if(kt->con == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    deltask(kt);
    return(NULL);
  }

  if(pipe(pp[0]) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    close(kt->con);
    deltask(kt);
    return(NULL);
  }

  if(pipe(pp[1]) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    close(kt->con);
    close(pp[0][0]);
    close(pp[0][1]);
    deltask(kt);
    return(NULL);
  }

  kt->pid = fork();
  if(kt->pid == -1){
    mtnlogger(mtn, 0, "[error] %s: fork error %s\n", __func__, strerror(errno));
    close(kt->con);
    close(pp[0][0]);
    close(pp[0][1]);
    close(pp[1][0]);
    close(pp[1][1]);
    deltask(kt);
    return(NULL);
  }

  if(kt->pid){
    kt->rpp = pp[0][0];
    kt->wpp = pp[1][1];
    close(pp[0][1]);
    close(pp[1][0]);
    close(kt->con);
    return(kt);
  }

  //----- child process -----
  kt->rpp = pp[1][0];
  kt->wpp = pp[0][1];
  close(pp[0][0]);
  close(pp[1][1]);
  while(ctx->cldtask){
    ctx->cldtask = deltask(ctx->cldtask);
  }
  fcntl(kt->con, F_SETFD, FD_CLOEXEC);
  fcntl(kt->rpp, F_SETFD, FD_CLOEXEC);
  fcntl(kt->wpp, F_SETFD, FD_CLOEXEC);
  if(ctx->ioprio){
    if(ioprio_set(IOPRIO_WHO_PROCESS, getpid(), ctx->ioprio) == -1){
      mtnlogger(mtn, 0, "[error] %s: ioprio_set: %s\n", __func__, strerror(errno));
    }
  }
  mtnd_child(kt);
  close(kt->con);
  close(kt->rpp);
  close(kt->wpp);
  _exit(0);
}

/*
 * mode
 *   0: チェックしてすぐ抜ける
 *   1: 全プロセスが終了するまで待つ
 */
void mtnd_waitpid(int mode)
{
  int    opt;
  pid_t  pid;
  MTNTASK *t;

  opt = mode ? 0 : WNOHANG;
  if(mode && ctx->signal){
    for(t=ctx->cldtask;t;t=t->next){
      mtnlogger(mtn, 0, "[info] %s: send signal pid=%d sig=%d\n", __func__, t->pid, ctx->signal);
      kill(t->pid, ctx->signal);
    }
  }
  while(ctx->cldtask){
    pid = waitpid(-1, NULL, opt);
    if(pid <= 0){
      if(mode){
        continue;
      }else{
        break;
      }
    }
    for(t=ctx->cldtask;t;t=t->next){
      if(t->pid == pid){
        break;
      }
    }
    if(!t){
      if(mode){
        continue;
      }else{
        break;
      }
    }
    close(t->rpp);
    close(t->wpp);
    if(t == ctx->cldtask){
      ctx->cldtask = deltask(t);
    }else{
      deltask(t);
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
  int  r;
  char d;
  MTNTASK *n = NULL;
  struct timeval tv;
  struct epoll_event en;
  struct epoll_event ev[8];

  /*===== Main Loop =====*/
  while(is_loop){
    mtnd_waitpid(0);
    r = epoll_wait(e, ev, 8, 1000);
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
      if(mtnd_cld_process(ev[r].data.fd)){
        continue;
      }
      if(ev[r].data.fd == l){
        if((n = mtnd_accept_process(ev[r].data.fd))){
          en.data.fd = n->rpp;
          en.events  = EPOLLIN;
          if(epoll_ctl(e, EPOLL_CTL_ADD, n->rpp, &en) == -1){
            mtnlogger(mtn, 0, "[error] %s: epoll_ctl1: %s\n", __func__, strerror(errno));
            deltask(n);
          }else{
            if((n->next = ctx->cldtask)){
              ctx->cldtask->prev = n;
            }
            ctx->cldtask = n;
          }
        }
        continue;
      }
      if(ev[r].data.fd == ctx->fsig[0]){
        mtnd_waitpid(0);
        read(ctx->fsig[0], &d, 1);
        continue;
      }
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
    mtnlogger(mtn, 0, "[error] %s: epoll_ctl1: %s\n", __func__, strerror(errno));
    return(-1);
  }

  ev.data.fd = m;
  ev.events  = EPOLLIN;
  if(epoll_ctl(e, EPOLL_CTL_ADD, m, &ev) == -1){
    mtnlogger(mtn, 0, "[error] %s: epoll_ctl2: %s\n", __func__, strerror(errno));
    return(-1);
  }

  ev.data.fd = ctx->fsig[0];
  ev.events  = EPOLLIN;
  if(epoll_ctl(e, EPOLL_CTL_ADD, ctx->fsig[0], &ev) == -1){
    mtnlogger(mtn, 0, "[error] %s: epoll_ctl3: %s\n", __func__, strerror(errno));
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
      {"pid",          1, NULL, 'P'},
      {"help",         0, NULL, 'h'},
      {"version",      0, NULL, 'v'},
      {"export",       1, NULL, 'e'},
      {"execute",      1, NULL, 'E'},
      {"group",        1, NULL, 'g'},
      {"rdonly",       0, NULL, 'r'},
      {"low-priority", 0, NULL, 'L'},
      {0, 0, 0, 0}
    };
  return(opt);
}

void init_task()
{
  memset(taskfunc, 0, sizeof(taskfunc));
  taskfunc[MTNCMD_STARTUP]  = (MTNFSTASKFUNC)mtnd_startup_process;
  taskfunc[MTNCMD_SHUTDOWN] = (MTNFSTASKFUNC)mtnd_shutdown_process;
  taskfunc[MTNCMD_HELLO]    = (MTNFSTASKFUNC)mtnd_hello_process;
  taskfunc[MTNCMD_INFO]     = (MTNFSTASKFUNC)mtnd_info_process;
  taskfunc[MTNCMD_LIST]     = (MTNFSTASKFUNC)mtnd_list_process;
  taskfunc[MTNCMD_STAT]     = (MTNFSTASKFUNC)mtnd_stat_process;
  taskfunc[MTNCMD_TRUNCATE] = (MTNFSTASKFUNC)mtnd_truncate_process;
  taskfunc[MTNCMD_MKDIR]    = (MTNFSTASKFUNC)mtnd_mkdir_process;
  taskfunc[MTNCMD_RMDIR]    = (MTNFSTASKFUNC)mtnd_rm_process;
  taskfunc[MTNCMD_UNLINK]   = (MTNFSTASKFUNC)mtnd_rm_process;
  taskfunc[MTNCMD_RENAME]   = (MTNFSTASKFUNC)mtnd_rename_process;
  taskfunc[MTNCMD_SYMLINK]  = (MTNFSTASKFUNC)mtnd_symlink_process;
  taskfunc[MTNCMD_READLINK] = (MTNFSTASKFUNC)mtnd_readlink_process;
  taskfunc[MTNCMD_CHMOD]    = (MTNFSTASKFUNC)mtnd_chmod_process;
  taskfunc[MTNCMD_CHOWN]    = (MTNFSTASKFUNC)mtnd_chown_process;
  taskfunc[MTNCMD_UTIME]    = (MTNFSTASKFUNC)mtnd_utime_process;
  init_task_child();
}

void mtnd_startmsg()
{
  uint64_t dsize;
  uint64_t dfree;
  getstatd(&dfree, &dsize);
  mtnlogger(mtn, 0, "======= %s start =======\n", MODULE_NAME);
  mtnlogger(mtn, 0, "ver  : %s\n", PACKAGE_VERSION);
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
  mtnlogger(mtn, 0, "size : %6llu [MB]\n", (dsize + ctx->free_limit) / 1024 / 1024);
  mtnlogger(mtn, 0, "free : %6llu [MB]\n", (dfree + ctx->free_limit) / 1024 / 1024);
  mtnlogger(mtn, 0, "limit: %6llu [MB]\n", ctx->free_limit / 1024 / 1024);
  mtnlogger(mtn, 0, "mode : %s\n", ctx->rdonly ? "RDONLY" : "RDWR");
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

void signal_handler(int n, siginfo_t *info, void *ucs)
{
  char data;
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
    case SIGCHLD:
      data = 0;
      write(ctx->fsig[1], &data, 1);
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
  while((r = getopt_long(argc, argv, "hvng:E:e:l:m:p:P:D:r", get_optlist(), NULL)) != -1){
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

      case 'r':
        ctx->rdonly = 1;
        break;

      case 'L':
        ctx->ioprio = IOPRIO_PRIO_VALUE(IOPRIO_CLASS_IDLE, 7);
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
  sig.sa_sigaction = signal_handler;
  sig.sa_flags     = SA_SIGINFO;
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
  sig.sa_flags = SA_SIGINFO | SA_NOCLDSTOP;
  if(sigaction(SIGCHLD, &sig, NULL) == -1){
    mtnlogger(NULL, 0, "%s: sigaction error SIGCHLD\n", __func__);
    exit(1);
  }
}

int mtnd_init_pipe()
{
  pipe(ctx->fsig);
  fcntl(ctx->fsig[0], F_SETFD, FD_CLOEXEC);
  fcntl(ctx->fsig[1], F_SETFD, FD_CLOEXEC);
  fcntl(ctx->fsig[0], F_SETFL, O_NONBLOCK);
  fcntl(ctx->fsig[1], F_SETFL, O_NONBLOCK);
  return(0);
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
  mtnd_init_pipe();
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

