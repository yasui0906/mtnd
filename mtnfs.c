/*
 * mtnfs.c
 * Copyright (C) 2011 KLab Inc.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION 28
#include <stdio.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <fuse.h>
#include "mtn.h"
#include "common.h"
#include "mtnfs.h"

static struct cmdopt cmdopt;

int is_mtnstatus(const char *path, char *buff)
{
  int len = strlen(MTNFS_STATUSPATH);
  if(strlen(path) < len){
    return(0);
  }
  if(memcmp(path, MTNFS_STATUSPATH, len)){
    return(0);
  }
  if(path[len] == 0){
    if(buff){
      *buff = 0;
    }
  }else{
    if(path[len] != '/'){
      return(0);
    }
    if(buff){
      strcpy(buff, path + len + 1);
    }
  }
  return(1);
}

MTNSTAT *get_dircache(const char *path, int flag)
{
  MTNFS *mtnfs;
  MTNDIR   *md;
  MTNSTAT  *st;
  struct timeval tv;

  mtnfs = MTNFS_CONTEXT;
  pthread_mutex_lock(&(mtnfs->cache_mutex));
  gettimeofday(&tv, NULL);
  md = mtnfs->dircache;
  while(md){
    if((tv.tv_sec - md->tv.tv_sec) > 10){
      clrstat(md->st);
      md->st   = NULL;
      md->flag = 0;
    }
    if((tv.tv_sec - md->tv.tv_sec) > 60){
      if(md == mtnfs->dircache){
        md = (mtnfs->dircache = deldir(mtnfs->dircache));
      }else{
        md = deldir(md);
      }
      continue;
    }
    if(strcmp(md->path, path) == 0){
      break;
    }
    md = md->next;
  }
  if(md == NULL){
    md = newdir(path);
    if((md->next = mtnfs->dircache)){
      mtnfs->dircache->prev = md;
    }
    mtnfs->dircache = md;
  }
  st = (!flag || md->flag) ? cpstat(md->st) : NULL;
  pthread_mutex_unlock(&(mtnfs->cache_mutex));
  return(st);
}

void addstat_dircache(const char *path, MTNSTAT *st)
{
  MTNDIR   *md;
  MTNFS *mtnfs;

  mtnfs = MTNFS_CONTEXT;
  if(st == NULL){
    return;
  }
  pthread_mutex_lock(&(mtnfs->cache_mutex));
  md = mtnfs->dircache;
  while(md){
    if(strcmp(md->path, path) == 0){
      break;
    }
    md = md->next;
  }
  if(md == NULL){
    md = newdir(path);
    if((md->next = mtnfs->dircache)){
      mtnfs->dircache->prev = md;
    }
    mtnfs->dircache = md;
  }
  md->st = mgstat(md->st, st);
  gettimeofday(&(md->tv), NULL);
  pthread_mutex_unlock(&(mtnfs->cache_mutex));
}

void setstat_dircache(const char *path, MTNSTAT *st)
{
  MTNDIR   *md;
  MTNFS *mtnfs;

  mtnfs = MTNFS_CONTEXT;
  pthread_mutex_lock(&(mtnfs->cache_mutex));
  md = mtnfs->dircache;
  while(md){
    if(strcmp(md->path, path) == 0){
      break;
    }
    md = md->next;
  }
  if(md == NULL){
    md = newdir(path);
    if((md->next = mtnfs->dircache)){
      mtnfs->dircache->prev = md;
    }
    mtnfs->dircache = md;
  }
  clrstat(md->st);
  md->st   = cpstat(st);
  md->flag = st ? 1 : 0;
  gettimeofday(&(md->tv), NULL);
  pthread_mutex_unlock(&(mtnfs->cache_mutex));
}

//-------------------------------------------------------------
//
//-------------------------------------------------------------
static void *mtnfs_init(struct fuse_conn_info *conn)
{
  int i;
  MTN   *mtn   = mtn_init(MODULE_NAME);
  MTNFS *mtnfs = calloc(1, sizeof(MTNFS));

  mtnfs->mtn = mtn;
  getcwd(mtnfs->cwd, sizeof(mtnfs->cwd));
  if(cmdopt.port){
    mtn->mcast_port = atoi(cmdopt.port);
  }
  if(cmdopt.addr){
    strcpy(mtn->mcast_addr, cmdopt.addr);
  }
  if(cmdopt.group){
    mtn->groupstr = newstr(cmdopt.group);
    mtn->grouparg = splitstr(cmdopt.group, ",");
  }
  if(cmdopt.loglevel){
    mtn->loglevel = atoi(cmdopt.loglevel);
  }
  if(cmdopt.pid){
    if(*(cmdopt.pid) == '/'){
      strcpy(mtnfs->pid, cmdopt.pid);
    }else{
      sprintf(mtnfs->pid, "%s/%s", mtnfs->cwd, cmdopt.pid);
    }
  }
  mtn->logmode = cmdopt.dontfork ? MTNLOG_STDERR : MTNLOG_SYSLOG;
  mtnfs->file_mutex = malloc(sizeof(pthread_mutex_t) * mtn->max_open);
  for(i=0;i<mtn->max_open;i++){
    pthread_mutex_init(&(mtnfs->file_mutex[i]), NULL);
  }
  mtnlogger(mtn, 0, "========================\n");
  mtnlogger(mtn, 0, "%s start (ver %s)\n", MODULE_NAME, PACKAGE_VERSION);
  mtnlogger(mtn, 0, "MulticastIP: %s\n", mtn->mcast_addr);
  mtnlogger(mtn, 0, "PortNumber : %d\n", mtn->mcast_port);
  mtnlogger(mtn, 0, "MaxOpen    : %d\n", mtn->max_open);
if(mtn->groupstr){
  mtnlogger(mtn, 0, "Group      : %s\n", mtn->groupstr);
}
  mtnlogger(mtn, 0, "DebugLevel : %d\n", mtn->loglevel);
  mkpidfile(mtnfs->pid);
  return(mtnfs);
}

static void mtnfs_destroy(void *buff)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  rmpidfile(mtnfs->pid);
  mtnlogger(mtnfs->mtn, 0, "%s finished\n", MODULE_NAME);
}

static int mtnfs_getattr(const char *path, struct stat *stbuf)
{
  MTN     *mtn = MTN_CONTEXT;
  MTNSTAT *krt = NULL;
  MTNSTAT *kst = NULL;
  struct timeval tv;
  char  d[PATH_MAX];
  char  f[PATH_MAX];

  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  memset(stbuf, 0, sizeof(struct stat));
  if(strcmp(path, "/") == 0) {
    stbuf->st_mode  = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
    return(0);
  }
  if(is_mtnstatus(path, f)){
    if(f[0] == 0){
      stbuf->st_mode  = S_IFDIR | 0555;
      stbuf->st_nlink = 2;
      return(0);
    }
    gettimeofday(&tv, NULL);
    stbuf->st_mode  = S_IFREG | 0444;
    stbuf->st_atime = tv.tv_sec;
    stbuf->st_mtime = tv.tv_sec;
    stbuf->st_ctime = tv.tv_sec;
    if(strcmp(f, "members") == 0) {
      stbuf->st_size = set_mtnstatus_members(mtn);
      return(0);
    }
    if(strcmp(f, "debuginfo") == 0) {
      stbuf->st_size = set_mtnstatus_debuginfo(mtn);
      return(0);
    }
    if(strcmp(f, "loglevel") == 0) {
      stbuf->st_mode = S_IFREG | 0644;
      stbuf->st_size = set_mtnstatus_loglevel(mtn);
      return(0);
    }
    return(-ENOENT);
  }

  dirbase(path, d, f);
  krt = get_dircache(d, 0);
  for(kst=krt;kst;kst=kst->next){
    if(strcmp(kst->name, f) == 0){
      memcpy(stbuf, &(kst->stat), sizeof(struct stat));
      break;
    }
  }
  if(!kst){
    if((kst = mtn_stat(mtn, path))){
      addstat_dircache(d, kst);
      memcpy(stbuf, &(kst->stat), sizeof(struct stat));
    }
  }
  clrstat(krt);
  return(kst ? 0 : -ENOENT);
}

static int mtnfs_fgetattr(const char *path, struct stat *st, struct fuse_file_info *fi)
{
  int r;
  struct timeval tv;
  char  f[PATH_MAX];
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = MTN_CONTEXT;
  mtnlogger(mtn, 8, "[debug] %s: path=%s fh=%d\n", __func__, path, fi->fh);
  if(strcmp(path, "/") == 0) {
    st->st_mode  = S_IFDIR | 0755;
    st->st_nlink = 2;
    return(0);
  }
  if(is_mtnstatus(path, f)){
    if(f[0] == 0){
      st->st_mode  = S_IFDIR | 0555;
      st->st_nlink = 2;
      return(0);
    }  
    if(fi->fh){
      gettimeofday(&tv, NULL);
      st->st_mode  = S_IFREG | 0444;
      st->st_size  = strlen((const char *)(fi->fh));
      st->st_atime = tv.tv_sec;
      st->st_mtime = tv.tv_sec;
      st->st_ctime = tv.tv_sec;
      if(strcmp(f, "members") == 0) {
        return(0);
      }
      if(strcmp(f, "debuginfo") == 0) {
        return(0);
      }
      if(strcmp(f, "loglevel") == 0) {
        st->st_mode  = S_IFREG | 0644;
        return(0);
      }
    }
    return(-EBADF);
  }
  pthread_mutex_lock(&(mtnfs->file_mutex[fi->fh]));
  r = mtn_fgetattr(mtn, (int)(fi->fh), st);
  pthread_mutex_unlock(&(mtnfs->file_mutex[fi->fh]));
  if(r == -1){
    mtnlogger(mtn, 0, "[error] %s: mtn_callcmd %s\n", __func__, strerror(errno));
    return(-errno);
  }
  return(0);
}

static int mtnfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
  MTN     *mtn = MTN_CONTEXT;
  MTNSTAT *mrt = NULL;
  MTNSTAT *mst = NULL;
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  filler(buf, ".",  NULL, 0);
  filler(buf, "..", NULL, 0);
  if(strcmp("/", path) == 0){
    filler(buf, MTNFS_STATUSNAME, NULL, 0);
  }
  if(!strcmp(path, MTNFS_STATUSPATH)){
    filler(buf, "members",   NULL, 0);
    filler(buf, "debuginfo", NULL, 0);
    filler(buf, "loglevel",  NULL, 0);
  }else{
    mrt = get_dircache(path, 1);
    if(!mrt){
      mrt = mtn_list(mtn, path);
      setstat_dircache(path, mrt);
    }
    for(mst=mrt;mst;mst=mst->next){
      filler(buf, mst->name, NULL, 0);
    }
    clrstat(mrt);
  }
  return(0);
}

static int mtnfs_mkdir(const char *path, mode_t mode)
{
  MTN *mtn = MTN_CONTEXT;
  struct fuse_context *fuse = fuse_get_context();
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  return(mtn_mkdir(mtn, path, fuse->uid, fuse->gid));
}

static int mtnfs_unlink(const char *path)
{
  int  r;
  char d[PATH_MAX];
  MTN *mtn = MTN_CONTEXT;
  dirbase(path, d, NULL);
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  r = mtn_rm(mtn, path);
  setstat_dircache(d, NULL);
  return(r);
}

static int mtnfs_rmdir(const char *path)
{
  int  r;
  char d[PATH_MAX];
  MTN *mtn = MTN_CONTEXT;
  dirbase(path, d, NULL);
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  r = mtn_rm(mtn, path);
  setstat_dircache(d, NULL);
  return(r);
}

static int mtnfs_symlink(const char *oldpath, const char *newpath)
{
  int  r = 0;
  char d[PATH_MAX];
  MTN *mtn = MTN_CONTEXT;
  dirbase(newpath, d, NULL);
  mtnlogger(mtn, 8, "[debug] %s: oldpath=%s newpath=%s\n", __func__, oldpath, newpath);
  r = mtn_symlink(mtn, oldpath, newpath);
  setstat_dircache(d, NULL);
  return(r);
}

static int mtnfs_readlink(const char *path, char *buff, size_t size)
{
  MTN *mtn = MTN_CONTEXT;
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  return(mtn_readlink(mtn, path, buff, size));
}

static int mtnfs_rename(const char *old_path, const char *new_path)
{
  int  r;
  char d0[PATH_MAX];
  char d1[PATH_MAX];
  MTN *mtn = MTN_CONTEXT;
  dirbase(old_path, d0, NULL);
  dirbase(new_path, d1, NULL);
  mtnlogger(mtn, 8, "[debug] %s: old_path=%s new_path=%s\n", __func__, old_path, new_path);
  r = mtn_rename(mtn, old_path, new_path);
  setstat_dircache(d0, NULL);
  setstat_dircache(d1, NULL);
  return(r);
}

static int mtnfs_chmod(const char *path, mode_t mode)
{
  int r;
  char d[PATH_MAX];
  MTN *mtn = MTN_CONTEXT;
  dirbase(path, d, NULL);
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  r = mtn_chmod(mtn, path, mode);
  setstat_dircache(d, NULL);
  return(r);
}

static int mtnfs_chown(const char *path, uid_t uid, gid_t gid)
{
  int r;
  char d[PATH_MAX];
  MTN *mtn = MTN_CONTEXT;
  dirbase(path, d, NULL);
  mtnlogger(mtn, 8, "[debug] %s: path=%s uid=%d gid=%d\n", __func__, path, uid, gid);
  r = mtn_chown(mtn, path, uid, gid);
  setstat_dircache(d, NULL);
  return(r);
}

static int mtnfs_utimens(const char *path, const struct timespec tv[2])
{
  int  r;
  char d[PATH_MAX];
  MTN *mtn = MTN_CONTEXT;
  dirbase(path, d, NULL);
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  r = mtn_utime(mtn, path, tv[0].tv_sec, tv[1].tv_sec);
  setstat_dircache(d, NULL);
  return(r);
}

static int mtnfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  int r; 
  MTNSTAT st;
  MTN *mtn = MTN_CONTEXT;
  mtnlogger(mtn, 8, "[debug] %s: path=%s fh=%llu mode=%o\n", __func__, path, fi->fh, mode);
  if(is_mtnstatus(path, NULL)){
    return(-EACCES);
  }
  st.stat.st_mode = mode;
  st.stat.st_uid  = FUSE_UID;
  st.stat.st_gid  = FUSE_GID;
  r = mtn_open(mtn, path, fi->flags, &st);
  if(r == -1){
    return(-errno);
  }
  fi->fh = (uint64_t)r;
  return(0);
}

static int mtnfs_open(const char *path, struct fuse_file_info *fi)
{
  int r;
  MTN *mtn = MTN_CONTEXT;
  char f[PATH_MAX];
  MTNSTAT st;

  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  if(is_mtnstatus(path, f)){
    if(!strcmp("members", f)){
      fi->fh = (uint64_t)get_mtnstatus_members(mtn);
    }else if(!strcmp("debuginfo", f)){
      fi->fh = (uint64_t)get_mtnstatus_debuginfo(mtn);
    }else if(!strcmp("loglevel", f)){
      fi->fh = (uint64_t)get_mtnstatus_loglevel(mtn);
    }else{
      return(-ENOENT);
    }
    return(0);
  }
  st.stat.st_mode = 0777;
  st.stat.st_uid  = FUSE_UID;
  st.stat.st_gid  = FUSE_GID;
  r = mtn_open(mtn, path, fi->flags, &st);
  fi->fh = (uint64_t)r;
  return((r == -1) ? -errno : 0);
}

static int mtnfs_truncate(const char *path, off_t offset)
{
  char f[PATH_MAX];
  MTN *mtn = MTN_CONTEXT;
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  if(is_mtnstatus(path, f)){
    if(!strcmp("loglevel", f)){
      return(0);
    }
    return(-EACCES);
  }
  return(mtn_truncate(mtn, path, offset));
}

static int mtnfs_release(const char *path, struct fuse_file_info *fi)
{
  int  r = 0;
  char f[PATH_MAX];
  MTN *mtn = MTN_CONTEXT;
  mtnlogger(mtn, 8, "[debug] %s: path=%s fh=%llu\n", __func__, path, fi->fh);
  if(is_mtnstatus(path, f)){
    if(!strcmp("members", f)){
      free_mtnstatus_members((char *)(fi->fh));
      fi->fh = 0;
    }else if(strcmp("debuginfo", f) == 0){
      free_mtnstatus_debuginfo((char *)(fi->fh));
      fi->fh = 0;
    }else if(strcmp("loglevel", f) == 0){
      mtn->loglevel = atoi((char *)(fi->fh));
      free_mtnstatus_loglevel((char *)(fi->fh));
      fi->fh = 0;
    }else{
      r = -EBADF;
    }
    return(r);
  }
  if(fi->fh){
    if(mtn_close(mtn, fi->fh) != 0){
      r = -errno;
    }
    fi->fh = 0;
  }
  return(r);
}

static int mtnfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  int r;
  int l;
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = MTN_CONTEXT;
  mtnlogger(mtn, offset ? 9 : 8, "[debug] %s: path=%s %u-%u (%u)\n", __func__, path, offset, offset + size, size);
  if(is_mtnstatus(path, NULL)){
    l = fi->fh ? strlen((void *)(fi->fh)) : 0;
    l = (offset < l) ? l - offset : 0;
    r = (size > l) ? l : size;
    if(r){
      memcpy(buf, (char *)(fi->fh) + offset, r);
    }
    return(r);
  }
  pthread_mutex_lock(&(mtnfs->file_mutex[fi->fh]));
  r = mtn_read(mtn, (int)(fi->fh), buf, size, offset);
  if(r < 0){
    mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(-r), path);
  }
  pthread_mutex_unlock(&(mtnfs->file_mutex[fi->fh]));
  return(r);
}

static int mtnfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  int r;
  MTN   *mtn   = MTN_CONTEXT;
  MTNFS *mtnfs = MTNFS_CONTEXT;
  mtnlogger(mtn, offset ? 9 : 8, "[debug] %s: path=%s %u-%u (%u)\n", __func__, path, offset, offset + size, size);
  if(is_mtnstatus(path, NULL)){
    fi->fh = (uint64_t)realloc((void *)(fi->fh), size);
    memcpy((void *)(fi->fh), buf, size);
    return(size); 
  }
  pthread_mutex_lock(&(mtnfs->file_mutex[fi->fh]));
  r = mtn_write(mtn, (int)(fi->fh), buf, size, offset); 
  if(r < 0){
    mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(-r), path);
  }
  pthread_mutex_unlock(&(mtnfs->file_mutex[fi->fh]));
  return(r);
}

static int mtnfs_statfs(const char *path, struct statvfs *sv)
{
  MTN *mtn = MTN_CONTEXT;
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  MTNSVR *m;
  MTNSVR *km = mtn_info(mtn);
  statvfs("/", sv);
  sv->f_blocks = 0;
  sv->f_bfree  = 0;
  sv->f_bavail = 0;
  for(m=km;m;m=m->next){
    sv->f_blocks += (m->dsize * m->fsize) / sv->f_frsize;
    if(m->dfree * m->bsize > m->limit){
      sv->f_bfree  += ((m->dfree * m->bsize) - m->limit) / sv->f_bsize;
      sv->f_bavail += ((m->dfree * m->bsize) - m->limit) / sv->f_bsize;
    }
  }
  clrsvr(km);
  return(0);
}

static int mtnfs_opendir(const char *path, struct fuse_file_info *fi)
{
  MTN *mtn = MTN_CONTEXT;
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_releasedir(const char *path, struct fuse_file_info *fi)
{
  MTN *mtn = MTN_CONTEXT;
  mtnlogger(mtn, 8, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
  MTN *mtn = MTN_CONTEXT;
  mtnlogger(mtn, 0, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_flush(const char *path, struct fuse_file_info *fi)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 9, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_fsync(const char *path, int sync, struct fuse_file_info *fi)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "[debug] %s: path=%s sync=%d\n", __func__, path, sync);
  return(0);
}

//-------------------------------------------------------------------------------------
// 以下未実装
//-------------------------------------------------------------------------------------
/*
static int mtnfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "%s: path=%s flags=%d\n", __func__, path, flags);
  return(0);
}

static int mtnfs_getxattr(const char *path, const char *name, char *value, size_t size)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_listxattr(const char *path, char *list, size_t size)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_removexattr(const char *path, const char *name)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_fsyncdir(const char *path, int flags, struct fuse_file_info *fi)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_access(const char *path, int mode)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_lock(const char *path, struct fuse_file_info *fi, int cmd, struct flock *l)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "[debug] %s: path=%s cmd=%d\n", __func__, path, cmd);
  return(0);
}

static int mtnfs_bmap(const char *path, size_t blocksize, uint64_t *idx)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_ioctl(const char *path, int cmd, void *arg, struct fuse_file_info *fi, unsigned int flags, void *data)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnfs_poll(const char *path, struct fuse_file_info *fi, struct fuse_pollhandle *ph, unsigned *reventsp)
{
  MTNFS *mtnfs = MTNFS_CONTEXT;
  MTN   *mtn   = mtnfs->mtn;
  mtnlogger(mtn, 0, "%s: path=%s\n", __func__, path);
  return(0);
}
*/

static struct fuse_operations mtn_oper = {
  .getattr     = mtnfs_getattr,
  .readlink    = mtnfs_readlink,
  .readdir     = mtnfs_readdir,
  .create      = mtnfs_create,
  .open        = mtnfs_open,
  .read        = mtnfs_read,
  .write       = mtnfs_write,
  .release     = mtnfs_release,
  .unlink      = mtnfs_unlink,
  .rmdir       = mtnfs_rmdir,
  .symlink     = mtnfs_symlink,
  .mkdir       = mtnfs_mkdir,
  .rename      = mtnfs_rename,
  .chmod       = mtnfs_chmod,
  .chown       = mtnfs_chown,
  .truncate    = mtnfs_truncate,
  .statfs      = mtnfs_statfs,
  .opendir     = mtnfs_opendir,
  .releasedir  = mtnfs_releasedir,
  .init        = mtnfs_init,
  .destroy     = mtnfs_destroy,
  .fgetattr    = mtnfs_fgetattr,
  .utimens     = mtnfs_utimens,
  .ftruncate   = mtnfs_ftruncate,
  .flush       = mtnfs_flush,
  .fsync       = mtnfs_fsync,
  //.fsyncdir    = mtnfs_fsyncdir,
  //.access      = mtnfs_access,
  //.setxattr    = mtnfs_setxattr,
  //.getxattr    = mtnfs_getxattr,
  //.listxattr   = mtnfs_listxattr,
  //.removexattr = mtnfs_removexattr,
  //.lock        = mtnfs_lock,
  //.bmap        = mtnfs_bmap,
  //.ioctl       = mtnfs_ioctl,
  //.poll        = mtnfs_poll,
};

#define MTNFS_OPT_KEY(t, p) { t, offsetof(struct cmdopt, p), 0 }
static struct fuse_opt mtnfs_opts[] = {
  MTNFS_OPT_KEY("-m %s",    addr),
  MTNFS_OPT_KEY("-p %s",    port),
  MTNFS_OPT_KEY("-g %s",    group),
  MTNFS_OPT_KEY("-d %s",    loglevel),
  MTNFS_OPT_KEY("--pid=%s", pid),
  FUSE_OPT_KEY("-h",        1),
  FUSE_OPT_KEY("--help",    1),
  FUSE_OPT_KEY("--version", 2),
  FUSE_OPT_KEY("-f",        3),
  FUSE_OPT_END
};

static int mtnfs_opt_parse(void *data, const char *arg, int key, struct fuse_args *outargs)
{
  struct cmdopt *opt = data;
  if(key == 1){
    fprintf(stderr, "usage: mtnfs mountpoint [options]\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "mtnfs options:\n");
    fprintf(stderr, "    -m IPADDR              Multicast Address(default: 224.0.0.110)\n");
    fprintf(stderr, "    -p PORT                Server Port(default: 6000)\n");
    fprintf(stderr, "    --pid=path             pid file(ex: /var/run/mtnfs.pid)\n");
    fprintf(stderr, "\n");
    return fuse_opt_add_arg(outargs, "-ho");
  }
  if(key == 2){
    fprintf(stderr, "%s version: %s\n", MODULE_NAME, PACKAGE_VERSION);
  }
  if(key == 3){
    opt->dontfork = 1;
  }
  return(1);
}

int main(int argc, char *argv[])
{
  memset(&cmdopt, 0, sizeof(cmdopt));
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  fuse_opt_parse(&args, &cmdopt, mtnfs_opts, mtnfs_opt_parse);
  fuse_opt_add_arg(&args, "-oallow_other");
  fuse_opt_add_arg(&args, "-odefault_permissions");
  return(fuse_main(args.argc, args.argv, &mtn_oper, NULL));
}

