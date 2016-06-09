//
// mtnd.c
// Copyright (C) 2012 KLab Inc.
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
#include <grp.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sched.h>
#include "mtnd.h"

static MTNFSTASKFUNC taskfunc[MTNCMD_MAX];

//------------------------------------------------------
// mtnfile commands for TCP
//------------------------------------------------------
static void mtnd_child_get(MTNTASK *kt)
{
  mtnlogger(mtn, 7, "[debug] %s: IN\n", __func__);
  if(kt->fd == -1){
    errno = EBADF;
    mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNCMD_ERROR;
    kt->send.head.size = 0;
    kt->send.head.flag = 0;
    kt->send.head.fin  = 1;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }
  while(is_loop){
    kt->send.head.size = read(kt->fd, kt->send.data.data, sizeof(kt->send.data.data));
    if(kt->send.head.size == 0){ // EOF
      close(kt->fd);
      kt->send.head.fin = 1;
      kt->fd = -1;
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
  mtnlogger(mtn, 7, "[debug] %s: OUT\n", __func__);
}

static void mtnd_child_put(MTNTASK *kt)
{
  if(kt->fd == -1){
    errno = EBADF;
    mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNCMD_ERROR;
    kt->send.head.size = 0;
    kt->send.head.flag = 0;
    kt->send.head.fin  = 1;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    return;
  }
  mtnlogger(mtn, 8, "[debug] %s: IN\n", __func__);
  if(kt->recv.head.size == 0){
    //----- EOF -----
    mtnlogger(mtn, 7, "[debug] %s: EOF\n", __func__);
    kt->send.head.fin = 1;
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
  mtnlogger(mtn, 8, "[debug] %s: OUT\n", __func__);
}

//------------------------------------------------------
// mtnfs commands for TCP
//------------------------------------------------------
static void mtnd_child_open(MTNTASK *kt)
{
  int    flags;
  mode_t  mode;
  MTNDATA data;
  char d[PATH_MAX];
  char f[PATH_MAX];

  mtnlogger(mtn, 7, "[debug] %s: IN\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtndata_get_int(&flags, &(kt->recv), sizeof(flags));
  if(ctx->ioprio){
    flags |= O_SYNC;
  }
  mtndata_get_int(&mode,  &(kt->recv), sizeof(mode));
  mtnd_fix_path(kt->path, NULL);
  dirbase(kt->path, d, f);
  if(mkdir_ex(d) == -1){
    mtnlogger(mtn, 0,"[error] %s: mkdir error %s %s\n", __func__, strerror(errno), d);
    kt->fin = 1;
    kt->send.head.fin = 1;
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
  }
  kt->fd = open(kt->path, flags, mode);
  if(kt->fd == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    mtnlogger(mtn, 0, "[error] %s: %s, path=%s create=%d mode=%o\n", __func__, strerror(errno), kt->path, ((flags & O_CREAT) != 0), mode);
  }else{
    fstat(kt->fd, &(kt->stat));
    memset(&data, 0, sizeof(data));
    data.head.ver  = PROTOCOL_VERSION;
    data.head.type = MTNCMD_OPEN;
    data.head.size = 0;
    mtndata_set_int(&(kt->init.use), &data, sizeof(kt->init.use));
    mtndata_set_string(kt->path, &data);
    if(send_data_stream(mtn, kt->wpp, &data) == -1){
      mtnlogger(mtn, 0, "[error]  %s: %s\n", __func__, strerror(errno));
    }
  }
  mtnlogger(mtn, 7, "[debug] %s: OUT\n", __func__);
}

static void mtnd_child_read(MTNTASK *kt)
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

static void mtnd_child_write(MTNTASK *kt)
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

static void mtnd_child_close(MTNTASK *kt)
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

static void mtnd_child_truncate(MTNTASK *kt)
{
  off_t offset;
  mtndata_get_string(kt->path, &(kt->recv));
  mtndata_get_int(&offset, &(kt->recv), sizeof(offset));
  mtnd_fix_path(kt->path, NULL);
  stat(kt->path, &(kt->stat));
  if(truncate(kt->path, offset) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    mtnlogger(mtn, 0, "[error] %s: %s path=%s offset=%llu\n", __func__, strerror(errno), kt->path, offset);
  }else{
    mtnlogger(mtn, 2, "[debug] %s: path=%s offset=%llu\n", __func__, kt->path, offset);
  }
}

static void mtnd_child_mkdir(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: START\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path, NULL);
  if(mkdir(kt->path, 0777) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtndata_set_int(&errno, &(kt->send), sizeof(errno));
  }
  mtnlogger(mtn, 8, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  mtnlogger(mtn, 9, "[debug] %s: END\n", __func__);
}

static void mtnd_child_rmdir(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: START\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path, NULL);
  if(rmdir(kt->path) == -1){
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtndata_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  mtnlogger(mtn, 8, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  mtnlogger(mtn, 9, "[debug] %s: END\n", __func__);
}

static void mtnd_child_unlink(MTNTASK *kt)
{
  mtnlogger(mtn, 9, "[debug] %s: START\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path, NULL);
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

static void mtnd_child_getattr(MTNTASK *kt)
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

static void mtnd_child_chmod(MTNTASK *kt)
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

static void mtnd_child_chown(MTNTASK *kt)
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

static void mtnd_child_setattr(MTNTASK *kt)
{
  mtnlogger(mtn, 0,"[debug] %s: START\n", __func__);
  mtndata_get_string(kt->path, &(kt->recv));
  mtnlogger(mtn, 0,"[debug] %s: END\n", __func__);
}

static void mtnd_child_result(MTNTASK *kt)
{
  memcpy(&(kt->send), &(kt->keep), sizeof(MTNDATA));
  kt->keep.head.type = MTNCMD_SUCCESS;
  kt->keep.head.size = 0;
}

static void mtnd_child_init_export(MTNTASK *kt)
{
  MTNSVR mb;
  if(!ctx->export || !strlen(ctx->cwd)){
    kt->fin = 1;
    errno = EPERM;
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }

  memset(&mb, 0, sizeof(mb));
  getstatd(&(mb.dfree), &(mb.dsize));
  if(kt->init.use > mb.dfree){
    kt->fin = 1;
    errno = ENOSPC;
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }
}

static void mtnd_child_init_exec(MTNTASK *kt)
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

static void mtnd_child_init(MTNTASK *kt)
{
  int r = 0;
  uid_t uid;
  gid_t gid;

  uid = getuid();
  gid = getgid();
  r += mtndata_get_int(&(kt->init.uid),  &(kt->recv), sizeof(kt->init.uid));
  r += mtndata_get_int(&(kt->init.gid),  &(kt->recv), sizeof(kt->init.gid));
  r += mtndata_get_int(&(kt->init.mode), &(kt->recv), sizeof(kt->init.mode)); 
  r += mtndata_get_int(&(kt->init.use),  &(kt->recv), sizeof(kt->init.use)); 
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

static void mtnd_child_exit(MTNTASK *kt)
{
  kt->fin = 1;
}

static void mtnd_child_fork(MTNTASK *kt, MTNJOB *job)
{
  struct sched_param schparam;
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
  mtn->logtype = 0;
  mtn->logmode = MTNLOG_STDERR;
  if(setpgid(0, 0) == -1){
    mtnlogger(mtn, 0, "[error] %s: setpgid %s\n", ctx->host, strerror(errno));
  }
  schparam.sched_priority = 0;
  if(sched_setscheduler(0, SCHED_BATCH, &schparam) == -1){
    mtnlogger(mtn, 0, "[error] %s: setscheduler %s\n", ctx->host, strerror(errno));
  }
  execlp("/bin/sh", "/bin/sh", "-c", job->cmd, NULL);
  mtnlogger(mtn, 0, "[error] %s: %s %s\n", ctx->host, strerror(errno), job->cmd);
  _exit(127);
}

static int mtnd_child_exec_sche(MTNTASK *kt, MTNJOB *job)
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

static void mtnd_child_exec_poll(MTNTASK *kt, MTNJOB *job, int wtime)
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

static void mtnd_child_exec(MTNTASK *kt)
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
  mtnlogger(mtn, 1, "[debug] %s: %s\n", __func__, job.cmd);

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
  mtnlogger(mtn, 1, "[debug] %s: exit_code=%d\n", __func__, job.exit);
  job_close(&job);
  if(kt->std[0]){
    close(kt->std[0]);
    kt->std[0] = 0;
  }
  mtnlogger(mtn, 8, "[debug] %s: return\n", __func__);
}

static void mtnd_child_rdonly(MTNTASK *kt)
{
  int flag;
  MTNDATA data;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  mtndata_get_int(&flag, &(kt->recv), sizeof(flag));
  memset(&data, 0, sizeof(data));
  data.head.ver  = PROTOCOL_VERSION;
  data.head.type = MTNCMD_RDONLY;
  data.head.size = 0;
  mtndata_set_int(&(flag), &data, sizeof(flag));
  if(send_data_stream(mtn, kt->wpp, &data) == -1){
    mtnlogger(mtn, 0, "[error]  %s: %s\n", __func__, strerror(errno));
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_child_error(MTNTASK *kt)
{
  errno = EACCES;
  mtnlogger(mtn, 0, "[error] %s: TYPE=%u\n", __func__, kt->recv.head.type);
  kt->send.head.type = MTNCMD_ERROR;
  mtndata_set_int(&errno, &(kt->send), sizeof(errno));
}

void init_task_child()
{
  memset(taskfunc, 0, sizeof(taskfunc));
  taskfunc[MTNCMD_PUT]      = mtnd_child_put;
  taskfunc[MTNCMD_GET]      = mtnd_child_get;
  taskfunc[MTNCMD_OPEN]     = mtnd_child_open;
  taskfunc[MTNCMD_READ]     = mtnd_child_read;
  taskfunc[MTNCMD_WRITE]    = mtnd_child_write;
  taskfunc[MTNCMD_CLOSE]    = mtnd_child_close;
  taskfunc[MTNCMD_TRUNCATE] = mtnd_child_truncate;
  taskfunc[MTNCMD_MKDIR]    = mtnd_child_mkdir;
  taskfunc[MTNCMD_RMDIR]    = mtnd_child_rmdir;
  taskfunc[MTNCMD_UNLINK]   = mtnd_child_unlink;
  taskfunc[MTNCMD_CHMOD]    = mtnd_child_chmod;
  taskfunc[MTNCMD_CHOWN]    = mtnd_child_chown;
  taskfunc[MTNCMD_GETATTR]  = mtnd_child_getattr;
  taskfunc[MTNCMD_SETATTR]  = mtnd_child_setattr;
  taskfunc[MTNCMD_RESULT]   = mtnd_child_result;
  taskfunc[MTNCMD_INIT]     = mtnd_child_init;
  taskfunc[MTNCMD_EXIT]     = mtnd_child_exit;
  taskfunc[MTNCMD_EXEC]     = mtnd_child_exec;
  taskfunc[MTNCMD_RDONLY]   = mtnd_child_rdonly;
}

void mtnd_child(MTNTASK *kt)
{
  char cmd[PATH_MAX + 16];
  mtnlogger(mtn, 1, "[debug] %s: accept from %s sock=%d\n", __func__, v4apstr(&(kt->addr)), kt->con);
  kt->keep.head.ver  = PROTOCOL_VERSION;
  kt->keep.head.type = MTNCMD_SUCCESS;
  kt->keep.head.size = 0;
  while(is_loop && !kt->fin){
    kt->send.head.ver  = PROTOCOL_VERSION;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->res = recv_data_stream(mtn, kt->con, &(kt->recv));
    if(kt->res == 1){
      mtnlogger(mtn, 0, "[error] %s: %s Connection reset by peer sock=%d\n", __func__, v4apstr(&(kt->addr)), kt->con);
      break;
    }
    if(kt->res == -1){
      mtnlogger(mtn, 0, "[error] %s: recv error type=%d sock=%d\n", __func__, kt->recv.head.type, kt->con);
      break;
    }
    if(!kt->init.init && (kt->recv.head.type != MTNCMD_INIT)){
      mtnlogger(mtn, 0, "[error] %s: not init: TYPE=%d\n", __func__, kt->recv.head.type);
      break;
    }
    if(taskfunc[kt->recv.head.type]){
      taskfunc[kt->recv.head.type](kt);
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

