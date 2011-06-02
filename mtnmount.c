#define FUSE_USE_VERSION 28
#include "mtnfs.h"
#include <fuse.h>

kdir *dir_cache = NULL;
pthread_mutex_t mtn_read_mutex  = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mtn_write_mutex = PTHREAD_MUTEX_INITIALIZER;

static int mtnmount_getattr(const char *path, struct stat *stbuf)
{
  int res = 0;
  char b[PATH_MAX];
  char d[PATH_MAX];
  char f[PATH_MAX];
  kdir *kd = dir_cache;
  kstat *krt = NULL;
  kstat *kst = NULL;

  strcpy(b, path);
  strcpy(d, dirname(b));
  strcpy(b, path);
  strcpy(f, basename(b));
  lprintf(0, "%s: dir=%s file=%s\n", __func__, d, f);
  memset(stbuf, 0, sizeof(struct stat));
  if(strcmp(path, "/") == 0) {
    stbuf->st_mode = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
    return(0);
  }
  while(kd){
    if(strcmp(kd->path, d) == 0){
      break;
    }
    kd = kd->next;
  }
  if(kd){
    krt = kd->kst;
  }else{ 
    krt = mtn_list(path);
  }
  for(kst=krt;kst;kst=kst->next){  
    if(strcmp(kst->name, f) == 0){
      break;
    }
  }
  if(kst){
    memcpy(stbuf, &(kst->stat), sizeof(struct stat));
    return(0);
  }
  return(-ENOENT);
}

static int mtnmount_readlink(const char *path, char *buff, size_t size)
{
  lprintf(0, "%s: path=%s\n", __func__, path);
  return 0;
}

static int mtnmount_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
  lprintf(0, "%s: path=%s\n", __func__, path);
  mtn_hello();
  kstat *krt = mtn_list(path);
  kstat *kst = krt;

  filler(buf, ".",  NULL, 0);
  filler(buf, "..", NULL, 0);
  while(kst){
    printf("%s: name=%s\n", __func__, kst->name);
    filler(buf, kst->name, NULL, 0);
    kst = kst->next;
  }
  dir_cache = mkkdir(path, krt, dir_cache);
  return 0;
}

static int mtnmount_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  int f;
  lprintf(0, "%s: 1 path=%s fh=%llu mode=%o\n", __func__, path, fi->fh, mode);
  f = mtn_open(path, fi->flags, mode);
  if(f == -1){
    return(-errno);
  }
  fi->fh = (uint64_t)f;
  lprintf(0, "%s: 2 path=%s fh=%llu\n", __func__, path, fi->fh);
  return(0); 
}

static int mtnmount_open(const char *path, struct fuse_file_info *fi)
{
  int f;
  lprintf(0, "%s: 1 path=%s fh=%llu\n", __func__, path, fi->fh);
  f = mtn_open(path, fi->flags, 0);
  if(f == -1){
    return(-errno);
  }
  fi->fh = (uint64_t)f;
  lprintf(0, "%s: 2 path=%s fh=%llu\n", __func__, path, fi->fh);
  return(0); 
}

static int mtnmount_release(const char *path, struct fuse_file_info *fi)
{
  lprintf(0, "%s: path=%s fh=%llu\n", __func__, path, fi->fh);
  if(fi->fh){
    int r = mtn_close(fi->fh);
    fi->fh = 0;
    if(r == 0){
      return(0);
    }
  }
  return(-EBADF);
}

static int mtnmount_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  pthread_mutex_lock(&mtn_read_mutex);
  lprintf(0,"%s: 1 pid=%d tid=%d path=%s size=%u offset=%llu\n", __func__, getpid(), syscall(SYS_gettid), path, size, offset);
  int r = mtn_read((int)(fi->fh), buf, size, offset); 
  lprintf(0,"%s: 2 pid=%d tid=%d read=%d\n", __func__, getpid(), syscall(SYS_gettid), r);
  pthread_mutex_unlock(&mtn_read_mutex);
  return r;
}

static int mtnmount_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  pthread_mutex_lock(&mtn_write_mutex);
  lprintf(0,"%s: 1 pid=%d tid=%d path=%s size=%u offset=%llu\n", __func__, getpid(), syscall(SYS_gettid), path, size, offset);
  int r = mtn_write((int)(fi->fh), buf, size, offset); 
  lprintf(0,"%s: 2 pid=%d tid=%d write=%d\n", __func__, getpid(), syscall(SYS_gettid), r);
  pthread_mutex_unlock(&mtn_write_mutex);
  return r;
}

static int mtnmount_truncate(const char *path, off_t offset)
{
  lprintf(0, "%s: path=%s offset=%llu\n", __func__, path, offset);
  if(mtn_truncate(path, offset) == -1){
    return(-errno);
  }
  return(0);
}

static int mtnmount_mkdir(const char *path, mode_t mode)
{
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_unlink(const char *path)
{
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_rmdir(const char *path)
{
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_rename(const char *old_path, const char *new_path)
{
  lprintf(0, "%s: old=%s new=%s\n", __func__, old_path, new_path);
  return(0);
}

static int mtnmount_chmod(const char *path, mode_t mode)
{
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_chown(const char *path, uid_t uid, gid_t gid)
{
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static struct fuse_operations mtn_oper = {
  .getattr  = mtnmount_getattr,
  .readlink = mtnmount_readlink,
  .readdir  = mtnmount_readdir,
  .create   = mtnmount_create,
  .open     = mtnmount_open,
  .read     = mtnmount_read,
  .write    = mtnmount_write,
  .release  = mtnmount_release,
  .unlink   = mtnmount_unlink,
  .rmdir    = mtnmount_rmdir,
  .mkdir    = mtnmount_mkdir,
  .rename   = mtnmount_rename,
  .chmod    = mtnmount_chmod,
  .chown    = mtnmount_chown,
  .truncate = mtnmount_truncate,
};

int main(int argc, char *argv[])
{
  mtn_init_option();
  mtn_hello();
  pthread_mutex_init(&mtn_read_mutex, NULL);
  pthread_mutex_init(&mtn_write_mutex, NULL);
  return fuse_main(argc, argv, &mtn_oper, NULL);
}

