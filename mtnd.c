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
MTNTASK *tasksave = NULL;
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
  printf("   -h         # help\n");
  printf("   -v         # version\n");
  printf("   -n         # no daemon\n");
  printf("   -e dir     # export dir\n");
  printf("   -m addr    # mcast addr\n");
  printf("   -p port    # TCP/UDP port(default: 6000)\n");
  printf("   -D num     # debug level\n");
  printf("   -l size    # limit size (MB)\n");
  printf("   --pid=path # pid file(ex: /var/run/mtnfs.pid)\n");
  printf("\n");
}

int get_diskfree(uint32_t *bsize, uint32_t *fsize, uint64_t *dsize, uint64_t *dfree)
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
  get_diskfree(&bsize, &fsize, &dsize, &dfree);
  dfree *= bsize;
  return(ctx->free_limit > dfree);
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

int cmp_task(MTNTASK *t1, MTNTASK *t2)
{
  if(cmpaddr(&(t1->addr), &(t2->addr)) != 0){
    return(1);
  }
  if(t1->recv.head.sqno != t2->recv.head.sqno){
    return(1);
  }
  return(0);
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
  if(!t){
    return(NULL);
  }
  n = cuttask(t);
  if(t == tasklist){
    tasklist = n;
  }
  if((t->next = tasksave)){
    t->next->prev = t;
  }
  tasksave = t;
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
  mtn_get_svrhost(mb, &(kt->recv));
  gettimeofday(&(mb->tv),NULL);
  kt->fin = 1;
  kt->send.head.type = MTNCMD_STARTUP;
  kt->send.head.size = 0;
  kt->send.head.flag = 1;
  mtn_set_string(ctx->host, &(kt->send));
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
  MTNTASK *t;
  uint32_t mcount;
  kt->fin = 1;
  for(t=tasksave;t;t=t->next){
    if(cmp_task(t, kt) == 0){
      break;
    }
  }
  if(t){
    kt->recv.head.flag = 1;
  }else{
    if(kt->recv.head.flag == 1){
      kt->fin = 2;
    }else{
      mcount = get_members_count(ctx->members);
      mtn_set_string(ctx->host, &(kt->send));
      mtn_set_int(&mcount, &(kt->send), sizeof(mcount));
    }
  }
}

static void mtnd_info_process(MTNTASK *kt)
{
  meminfo mi;
  MTNSVR mb;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  get_meminfo(&mi);
  mb.limit = ctx->free_limit;
  mb.malloccnt = getcount(MTNCOUNT_MALLOC);
  mb.membercnt = get_members_count(ctx->members);
  get_diskfree(&(mb.bsize), &(mb.fsize), &(mb.dsize), &(mb.dfree));
  mtn_set_int(&(mb.bsize),     &(kt->send), sizeof(mb.bsize));
  mtn_set_int(&(mb.fsize),     &(kt->send), sizeof(mb.fsize));
  mtn_set_int(&(mb.dsize),     &(kt->send), sizeof(mb.dsize));
  mtn_set_int(&(mb.dfree),     &(kt->send), sizeof(mb.dfree));
  mtn_set_int(&(mb.limit),     &(kt->send), sizeof(mb.limit));
  mtn_set_int(&(mb.malloccnt), &(kt->send), sizeof(mb.malloccnt));
  mtn_set_int(&(mb.membercnt), &(kt->send), sizeof(mb.membercnt));
  mtn_set_int(&(mi.vsz),       &(kt->send), sizeof(mi.vsz));
  mtn_set_int(&(mi.res),       &(kt->send), sizeof(mi.res));
  kt->send.head.fin = 1;
  kt->fin = 1;
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
    if(lstat(full, &(kt->stat)) == 0){
      len  = mtn_set_string(ent->d_name, NULL);
      len += mtn_set_stat(&(kt->stat),   NULL);
      if(kt->send.head.size + len <= mtn->max_packet_size){
        mtn_set_string(ent->d_name, &(kt->send));
        mtn_set_stat(&(kt->stat),   &(kt->send));
      }else{
        send_dgram(mtn, kt->con, &(kt->send), &(kt->addr));
        memset(&(kt->send), 0, sizeof(kt->send));
        mtn_set_string(ent->d_name, &(kt->send));
        mtn_set_stat(&(kt->stat),   &(kt->send));
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

static void mtnd_list_process(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  char buff[PATH_MAX];
  if(kt->dir){
    mtnd_list_dir(kt);
  }else{
    kt->fin = 1;
    kt->send.head.size = 0;
    mtn_get_string(buff, &(kt->recv));
    mtnd_fix_path(buff);
    sprintf(kt->path, "./%s", buff);
    if(lstat(kt->path, &(kt->stat)) == -1){
      if(errno != ENOENT){
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
      }
      kt->send.head.fin = 1;
    }
    if(S_ISREG(kt->stat.st_mode)){
      mtn_set_string(basename(kt->path), &(kt->send));
      mtn_set_stat(&(kt->stat), &(kt->send));
      kt->send.head.fin = 1;
    }
    if(S_ISDIR(kt->stat.st_mode)){
      if((kt->dir = opendir(kt->path))){
        kt->fin = 0;
      }else{
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUI\n", __func__);
}

static void mtnd_stat_process(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  char buff[PATH_MAX];
  char file[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(buff, &(kt->recv));
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
    mtn_set_string(file, &(kt->send));
    mtn_set_stat(&(kt->stat), &(kt->send));
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_truncate_process(MTNTASK *kt)
{
  off_t offset;
  char path[PATH_MAX];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.type = MTNCMD_SUCCESS;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(path, &(kt->recv));
  mtn_get_int(&offset, &(kt->recv), sizeof(offset));
  mtnd_fix_path(path);
  sprintf(kt->path, "./%s", path);
  if(truncate(kt->path, offset) == -1){
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
      mtnlogger(mtn, 0, "[error] %s: %s path=%s offset=%llu\n", __func__, strerror(errno), kt->path, offset);
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_mkdir_process(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  uid_t uid;
  gid_t gid;
  char buff[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(buff, &(kt->recv));
  mtn_get_int(&uid, &(kt->recv), sizeof(uid));
  mtn_get_int(&gid, &(kt->recv), sizeof(gid));
  mtnd_fix_path(buff);
  sprintf(kt->path, "./%s", buff);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(mkdir_ex(kt->path) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  chown(kt->path, uid, gid);
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_rm_process(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  char buff[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(buff, &(kt->recv));
  mtnd_fix_path(buff);
  sprintf(kt->path, "./%s", buff);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(lstat(kt->path, &(kt->stat)) != -1){
    if(S_ISDIR(kt->stat.st_mode)){
      if(rmdir(kt->path) == -1){
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        kt->send.head.type = MTNCMD_ERROR;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }else{
      if(unlink(kt->path) == -1){
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        kt->send.head.type = MTNCMD_ERROR;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_rename_process(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  char obuff[PATH_MAX];
  char nbuff[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(obuff, &(kt->recv));
  mtn_get_string(nbuff, &(kt->recv));
  mtnd_fix_path(obuff);
  mtnd_fix_path(nbuff);
  sprintf(kt->path, "./%s", obuff);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(rename(obuff, nbuff) == -1){
    if(errno != ENOENT){
      mtnlogger(mtn, 0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), obuff, nbuff);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_symlink_process(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  char oldpath[PATH_MAX];
  char newpath[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(oldpath, &(kt->recv));
  mtn_get_string(newpath, &(kt->recv));
  mtnd_fix_path(newpath);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(symlink(oldpath, newpath) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), oldpath, newpath);
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_readlink_process(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  ssize_t size;
  char newpath[PATH_MAX];
  char oldpath[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(newpath, &(kt->recv));
  mtnd_fix_path(newpath);
  kt->send.head.type = MTNCMD_SUCCESS;
  size = readlink(newpath, oldpath, PATH_MAX);
  if(size == -1){
    mtnlogger(mtn, 0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), oldpath, newpath);
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }else{
    oldpath[size] = 0;
    mtn_set_string(oldpath, &(kt->send));
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_chmod_process(MTNTASK *kt)
{
  mode_t mode;
  char path[PATH_MAX];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(path, &(kt->recv));
  mtn_get_int(&mode, &(kt->recv), sizeof(mode));
  mtnd_fix_path(path);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(chmod(path, mode) == -1){
    if(errno != ENOENT){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_chown_process(MTNTASK *kt)
{
  uid_t uid;
  gid_t gid;
  char path[PATH_MAX];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(path, &(kt->recv));
  mtn_get_int(&uid, &(kt->recv), sizeof(uid));
  mtn_get_int(&gid, &(kt->recv), sizeof(gid));
  mtnd_fix_path(path);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(chown(path, uid, gid) == -1){
    if(errno != ENOENT){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_utime_process(MTNTASK *kt)
{
  struct utimbuf ut;
  char path[PATH_MAX];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(path, &(kt->recv));
  mtn_get_int(&(ut.actime),  &(kt->recv), sizeof(ut.actime));
  mtn_get_int(&(ut.modtime), &(kt->recv), sizeof(ut.modtime));
  mtnd_fix_path(path);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(utime(path, &ut) == -1){
    if(errno != ENOENT){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
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
      mtn_set_int(&errno, &(t->send), sizeof(errno));
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
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }
  while(is_loop){
    kt->send.head.size = read(kt->fd, kt->send.data.data, sizeof(kt->send.data.data));
    if(kt->send.head.size == 0){ // EOF
      close(kt->fd);
      kt->send.head.fin = 1;
      kt->fd  = 0;
      break;
    }
    if(kt->send.head.size == -1){
      mtnlogger(mtn, 0,"[error] %s: file read error %s %s\n", __func__, strerror(errno), kt->path);
      kt->send.head.type = MTNCMD_ERROR;
      kt->send.head.size = 0;
      kt->send.head.fin  = 1;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
      break;
    }
    if(send_data_stream(mtn, kt->con, &(kt->send)) == -1){
      mtnlogger(mtn, 0,"[error] %s: send error %s %s\n", __func__, strerror(errno), kt->path);
      kt->send.head.type = MTNCMD_ERROR;
      kt->send.head.size = 0;
      kt->send.head.fin  = 1;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
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
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }else{
    //----- open/create -----
    mtn_get_string(kt->path,  &(kt->recv));
    mtn_get_stat(&(kt->stat), &(kt->recv));
    mtnlogger(mtn, 0,"[info]  %s: creat %s\n", __func__, kt->path);
    dirbase(kt->path, d, f);
    if(mkdir_ex(d) == -1){
      mtnlogger(mtn, 0,"[error] %s: mkdir error %s %s\n", __func__, strerror(errno), d);
      kt->fin = 1;
      kt->send.head.fin = 1;
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      kt->fd = creat(kt->path, kt->stat.st_mode);
      if(kt->fd == -1){
        mtnlogger(mtn, 0,"[error] %s: can't creat %s %s\n", __func__, strerror(errno), kt->path);
        kt->fd  = 0;
        kt->fin = 1;
        kt->send.head.fin = 1;
        kt->send.head.type = MTNCMD_ERROR;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
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
  mtn_get_string(kt->path, &(kt->recv));
  mtn_get_int(&flags, &(kt->recv), sizeof(flags));
  mtn_get_int(&mode,  &(kt->recv), sizeof(mode));
  mtnd_fix_path(kt->path);
  dirbase(kt->path, d, f);
  mkdir_ex(d);
  kt->fd = open(kt->path, flags, mode);
  if(kt->fd == -1){
    kt->fd = 0;
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
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
  mtn_get_int(&size,   &(kt->recv), sizeof(size));
  mtn_get_int(&offset, &(kt->recv), sizeof(offset));

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

  if(mtn_get_int(&offset, &(kt->recv), sizeof(offset)) == -1){
    errno = EIO;
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }
  size = kt->recv.head.size;
  buff = kt->recv.data.data;

  if(total > 1024 * 1024){
    if(is_freelimit()){
      errno = ENOSPC;
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
      return;
    }
    total = 0;
  }
  while(size){
    kt->res = pwrite(kt->fd, buff, size, offset);
    if(kt->res == -1){
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
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
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
    kt->fd = 0;
  }
}

void mtnd_child_truncate(MTNTASK *kt)
{
  off_t offset;
  mtn_get_string(kt->path, &(kt->recv));
  mtn_get_int(&offset, &(kt->recv), sizeof(offset));
  mtnd_fix_path(kt->path);
  stat(kt->path, &(kt->stat));
  if(truncate(kt->path, offset) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    mtnlogger(mtn, 0, "[error] %s: %s path=%s offset=%llu\n", __func__, strerror(errno), kt->path, offset);
  }else{
    mtnlogger(mtn, 2, "[debug] %s: path=%s offset=%llu\n", __func__, kt->path, offset);
  }
}

void mtnd_child_mkdir(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path);
  if(mkdir(kt->path, 0777) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  mtnlogger(mtn, 8, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  mtnlogger(mtn, 9, "[debug] %s: END\n", __func__);
}

void mtnd_child_rmdir(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path);
  if(rmdir(kt->path) == -1){
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  mtnlogger(mtn, 8, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  mtnlogger(mtn, 9, "[debug] %s: END\n", __func__);
}

void mtnd_child_unlink(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path);
  if(lstat(kt->path, &(kt->stat)) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    mtnlogger(mtn, 0,"[debug] %s: stat error: %s %s\n", __func__, strerror(errno), kt->path);
    return;
  }
  if(unlink(kt->path) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
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
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtn_set_stat(&st, &(kt->send));
      mtnlogger(mtn, 1, "[debug] %s: 2 size=%d\n", __func__, kt->send.head.size);
    }
  }else{
    mtn_get_string(kt->path, &(kt->recv));
    if(lstat(kt->path, &st) == -1){
      mtnlogger(mtn, 0, "[error] %s: 3\n", __func__);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtnlogger(mtn, 1, "[debug] %s: 4\n", __func__);
      mtn_set_stat(&st, &(kt->send));
    }
  }
  mtnlogger(mtn, 1, "[debug] %s: END\n", __func__);
}

void mtnd_child_chmod(MTNTASK *kt)
{
  uint32_t mode;
  mtnlogger(mtn, 0,"[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtn_get_int(&mode, &(kt->recv), sizeof(mode));
  if(chmod(kt->path, mode) == -1){
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }else{    
    if(stat(kt->path, &(kt->stat)) == -1){
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtn_set_stat(&(kt->stat), &(kt->send));
    }
  }
  mtnlogger(mtn, 0,"[debug] %s: END\n", __func__);
}

void mtnd_child_chown(MTNTASK *kt)
{
  uid_t uid;
  uid_t gid;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtn_get_int(&uid, &(kt->recv), sizeof(uid));
  mtn_get_int(&gid, &(kt->recv), sizeof(uid));
  if(chown(kt->path, uid, gid) == -1){
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }else{    
    if(stat(kt->path, &(kt->stat)) == -1){
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtn_set_stat(&(kt->stat), &(kt->send));
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

void mtnd_child_setattr(MTNTASK *kt)
{
  mtnlogger(mtn, 0,"[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnlogger(mtn, 0,"[debug] %s: END\n", __func__);
}

void mtnd_child_result(MTNTASK *kt)
{
  memcpy(&(kt->send), &(kt->keep), sizeof(MTNDATA));
  kt->keep.head.type = MTNCMD_SUCCESS;
  kt->keep.head.size = 0;
}

void mtnd_child_init_exec(MTNTASK *kt)
{
  if(!strlen(ctx->ewd)){
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
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
}

void mtnd_child_init(MTNTASK *kt)
{
  int r = 0;
  uid_t uid;
  gid_t gid;

  uid = getuid();
  gid = getgid();
  r += mtn_get_int(&(kt->init.uid),  &(kt->recv), sizeof(kt->init.uid));
  r += mtn_get_int(&(kt->init.gid),  &(kt->recv), sizeof(kt->init.gid));
  r += mtn_get_int(&(kt->init.mode), &(kt->recv), sizeof(kt->init.mode)); 
  if(r){
    mtnlogger(mtn, 0, "[error] %s: mtn protocol error\n", __func__);
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
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
    case 1:
      mtnd_child_init_exec(kt);
      break;
  }

  if(kt->fin){
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }

  kt->init.init = 1;
}

void mtnd_child_exit(MTNTASK *kt)
{
  kt->fin = 1;
}

void mtnd_child_exec(MTNTASK *kt)
{
  int e;
  int r;
  pid_t pid;
  int size;
  int status;
  int pp[3][2];
  struct epoll_event ev;
  char cmd[PATH_MAX];
  char buff[MTN_MAX_DATASIZE];
  mtn_get_string(cmd, &(kt->recv));
  mtnlogger(mtn, 0, "[debug] %s: %s\n", __func__, cmd);

  pipe(pp[0]);
  pipe(pp[1]);
  pipe(pp[2]);
  pid = fork();
  if(pid == -1){
    close(pp[0][0]);
    close(pp[0][1]);
    close(pp[1][0]);
    close(pp[1][1]);
    close(pp[2][0]);
    close(pp[2][1]);
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }
  if(pid){
    kt->std[0] = pp[0][1];
    kt->std[1] = pp[1][0];
    kt->std[2] = pp[2][0];
    close(pp[0][0]);
    close(pp[1][1]);
    close(pp[2][1]);
    e = epoll_create(4);
    ev.data.fd = kt->std[0];
    ev.events  = EPOLLOUT;
    epoll_ctl(e, EPOLL_CTL_ADD, kt->std[0], &ev);
    ev.data.fd = kt->std[1];
    ev.events  = EPOLLIN;
    epoll_ctl(e, EPOLL_CTL_ADD, kt->std[1], &ev);
    ev.data.fd = kt->std[2];
    ev.events  = EPOLLIN;
    epoll_ctl(e, EPOLL_CTL_ADD, kt->std[2], &ev);
    ev.data.fd = kt->con;
    ev.events  = EPOLLIN;
    epoll_ctl(e, EPOLL_CTL_ADD, kt->con, &ev);

    size = 0;
    kt->recv.head.size = 0;
    kt->send.head.size = 0;
    kt->send.head.ver  = PROTOCOL_VERSION;
    kt->send.head.type = MTNCMD_SUCCESS;
    send_data_stream(mtn, kt->con, &(kt->send));

    while(is_loop && (kt->std[1] || kt->std[2])){
      r = epoll_wait(e, &ev, 1, 1000);
      if(r != 1){
        continue;
      }
      if(ev.data.fd == kt->con){
        kill(pid, SIGINT);
        break;
      }
      if(ev.data.fd == kt->std[0]){
        if(size < kt->recv.head.size){
          r = write(kt->std[0], kt->recv.data.data + size, kt->recv.head.size - size);
          if(r == -1){
            mtnlogger(mtn, 0, "[error] %s: STDIN %s\n", __func__, strerror(errno));
          }else{
            size += r;
          }
        }else{
          size = 0;
          kt->send.head.size = 0;
          kt->send.head.type = MTNCMD_STDIN;
          send_recv_stream(mtn, kt->con, &(kt->send), &(kt->recv));
          mtnlogger(mtn, 0, "[debug] %s: STDIN  FD=%d SIZE=%d\n", __func__, kt->std[0], kt->recv.head.size);
          if(kt->recv.head.size == 0){
            epoll_ctl(e, EPOLL_CTL_DEL, kt->std[0], NULL);
            close(kt->std[0]);
            kt->std[0] = 0;
          }
        }
      }
      if(ev.data.fd == kt->std[1]){
        r = read(kt->std[1], buff, sizeof(buff));
        mtnlogger(mtn, 0, "[debug] %s: STDOUT FD=%d R=%d\n", __func__, kt->std[1],r );
        if(r > 0){
          kt->send.head.type = MTNCMD_STDOUT;
          kt->send.head.size = 0;
          mtn_set_data(buff, &(kt->send), r);
          send_data_stream(mtn, kt->con, &(kt->send));
        }
        if(r == 0){
          epoll_ctl(e, EPOLL_CTL_DEL, kt->std[1], NULL);
          close(kt->std[1]);
          kt->std[1] = 0;
        }
      }
      if(ev.data.fd == kt->std[2]){
        r = read(kt->std[2], buff, sizeof(buff));
        mtnlogger(mtn, 0, "[debug] %s: STDERR FD=%d R=%d\n", __func__, kt->std[2],r);
        if(r > 0){
          kt->send.head.type = MTNCMD_STDERR;
          kt->send.head.size = 0;
          mtn_set_data(buff, &(kt->send), r);
          send_data_stream(mtn, kt->con, &(kt->send));
        }
        if(r == 0){
          epoll_ctl(e, EPOLL_CTL_DEL, kt->std[2], NULL);
          close(kt->std[2]);
          kt->std[2] = 0;
        }
      }
    }
    while(waitpid(pid, &status, 0) != pid);
    close(e);
    if(kt->std[0]){
      close(kt->std[0]);
      kt->std[0] = 0;
    }
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    mtnlogger(mtn, 0, "[debug] %s: return\n", __func__);
    return;
  }

  //===== exec process =====
  close(0);
  close(1);
  close(2);
  close(pp[0][1]);
  close(pp[1][0]);
  close(pp[2][0]);
  dup2(pp[0][0],0); // stfdin
  dup2(pp[1][1],1); // stdout
  dup2(pp[2][1],2); // stderr
  execlp("sh", "sh", "-c", cmd, NULL);
  mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), cmd);
  _exit(127);
}

void mtnd_child_error(MTNTASK *kt)
{
  errno = EACCES;
  mtnlogger(mtn, 0, "[error] %s: TYPE=%u\n", __func__, kt->recv.head.type);
  kt->send.head.type = MTNCMD_ERROR;
  mtn_set_int(&errno, &(kt->send), sizeof(errno));
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
      case 0:
        break;
      case 1:
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
    exit(0);
  }

  kt.pid = fork();
  if(kt.pid == -1){
    mtnlogger(mtn, 0, "[error] %s: fork error %s\n", __func__, strerror(errno));
    close(kt.con);
    return;
  }
  if(kt.pid){
    close(kt.con);
    return;
  }

  //----- child process -----
  close(l);
  mtnd_child(&kt);
  close(kt.con);
  _exit(0);
}

void mtnd_loop(int e, int l)
{
  int      r;
  MTNTASK *t;
  MTNSVR  *m;
  struct timeval tv;
  struct timeval tv_health;
  struct epoll_event ev[2];
  gettimeofday(&tv_health, NULL);

  //===== Main Loop =====
  while(is_loop){
    waitpid(-1, NULL, WNOHANG);
    r = epoll_wait(e, ev, 2, 1000);
    gettimeofday(&tv, NULL);
    if((tv.tv_sec - tv_health.tv_sec) > 60){
      mtn_startup(mtn, 1);
      memcpy(&tv_health, &tv, sizeof(tv));
    }
    m = ctx->members;
    while(m){
      if((tv.tv_sec - m->tv.tv_sec) > 300){
        if(m == ctx->members){
          m = ctx->members = delsvr(m);
        }else{
          m = delsvr(m);
        }
        continue;
      }
      m = m->next;
    }
    if(r == 0){
      t = tasksave;
      while(t){
        if((tv.tv_sec - t->tv.tv_sec) > 30){
          if(t == tasksave){
            tasksave = t = deltask(t);
          }else{
            t = deltask(t);
          }
          continue;
        }
        t = t->next;
      }
      continue;
    }
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

void mtnd_main()
{
  int m;
  int l;
  int e;
  struct epoll_event ev;

  e = epoll_create(2);
  if(e == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }

  m = create_msocket(mtn);
  if(m == -1){
    mtnlogger(mtn, 0, "[error] %s: can't socket create\n", __func__);
    return;
  }

  l = create_lsocket(mtn);
  if(l == -1){
    mtnlogger(mtn, 0, "[error] %s: can't socket create\n", __func__);
    return;
  }

  if(listen(l, 64) == -1){
    mtnlogger(mtn, 0, "%s: listen error\n", __func__);
    return;
  }

  ev.data.fd = l;
  ev.events  = EPOLLIN;
  if(epoll_ctl(e, EPOLL_CTL_ADD, l, &ev) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }

  ev.data.fd = m;
  ev.events  = EPOLLIN;
  if(epoll_ctl(e, EPOLL_CTL_ADD, m, &ev) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }
  mtn_startup(mtn, 0);
  mtnd_loop(e, l);
  mtn_shutdown(mtn);
  close(e);
  close(m);
  close(l);
}

void daemonize()
{
  int pid;
  if(!ctx->daemonize){
    return;
  }
  pid = fork();
  if(pid == -1){
    mtnlogger(mtn, 0, "[error] %s: can't fork()\n", __func__);
    exit(1); 
  }
  if(pid){
    _exit(0);
  }
  setsid();
  pid = fork();
  if(pid == -1){
    mtnlogger(mtn, 0, "[error] %s: can't fork()\n", __func__);
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

void signal_handler(int n)
{
  switch(n){
    case SIGINT:
    case SIGTERM:
      is_loop = 0;
      mtn_break();
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

struct option *get_optlist()
{
  static struct option opt[]={
      {"pid",     1, NULL, 'P'},
      {"help",    0, NULL, 'h'},
      {"version", 0, NULL, 'v'},
      {"export",  1, NULL, 'e'},
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

void parse(int argc, char *argv[])
{
  int r;
  if(argc < 2){
    usage();
    exit(0);
  }
  while((r = getopt_long(argc, argv, "hvnE:e:l:m:p:P:D:", get_optlist(), NULL)) != -1){
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

      case 'E':
        strcpy(ctx->ewd, optarg);
        break;

      case 'e':
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

int mtnd()
{
  uint32_t bsize;
  uint32_t fsize;
  uint64_t dsize;
  uint64_t dfree;
  get_diskfree(&bsize, &fsize, &dsize, &dfree);
  mtnlogger(mtn, 0, "======= %s start =======\n", MODULE_NAME);
  mtnlogger(mtn, 0, "ver  : %s\n", MTN_VERSION);
  mtnlogger(mtn, 0, "pid  : %d\n", getpid());
  mtnlogger(mtn, 0, "log  : %d\n", mtn->loglevel);
  mtnlogger(mtn, 0, "addr : %s\n", mtn->mcast_addr);
  mtnlogger(mtn, 0, "port : %d\n", mtn->mcast_port);
  mtnlogger(mtn, 0, "host : %s\n", ctx->host);
  mtnlogger(mtn, 0, "base : %s\n", ctx->cwd);
  mtnlogger(mtn, 0, "exec : %s\n", ctx->ewd);
  mtnlogger(mtn, 0, "size : %6llu [MB]\n", fsize * dsize / 1024 / 1024);
  mtnlogger(mtn, 0, "free : %6llu [MB]\n", bsize * dfree / 1024 / 1024);
  mtnlogger(mtn, 0, "limit: %6llu [MB]\n", ctx->free_limit/1024 / 1024);
  mkpidfile(ctx->pid);
  mtnd_main();
  rmpidfile(ctx->pid);
  mtnlogger(mtn, 0, "%s finished\n", MODULE_NAME);
  return(0);
}

void mtnd_init()
{
  mtn = mtn_init(MODULE_NAME);
  mtn->logmode = MTNLOG_STDERR;
  ctx = malloc(sizeof(MTND));
  ctx->daemonize = 1;
  getcwd(ctx->cwd, sizeof(ctx->cwd));
  gethostname(ctx->host, sizeof(ctx->host));
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

