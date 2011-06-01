#define FUSE_USE_VERSION 26
#include "mtnfs.h"
#include <fuse.h>

kdir *dir_cache = NULL;

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
  printf("%s: dir=%s file=%s\n", __func__, d, f);
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
  printf("%s: path=%s\n", __func__, path);
  return 0;
}

static int mtnmount_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
  printf("%s: path=%s\n", __func__, path);
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
  printf("%s: path=%s fh=%llu\n", __func__, path, fi->fh);
  f = (uint64_t)mtn_create(path, fi->flags, mode);
  if(f == -1){
    return(-errno);
  }
  fi->fh = f;
  return(0); 
}

static int mtnmount_open(const char *path, struct fuse_file_info *fi)
{
  int f;
  printf("%s: path=%s fh=%llu\n", __func__, path, fi->fh);
  f = mtn_open(path, fi->flags);
  if(f == -1){
    return(-errno);
  }
  fi->fh = (uint64_t)f;
  return(0); 
}

static int mtnmount_release(const char *path, struct fuse_file_info *fi)
{
  printf("%s: path=%s fh=%llu\n", __func__, path, fi->fh);
  if(fi->fh){
    if(mtn_close(fi->fh) == 0){
      return(0);
    }
  }
  return(-EBADF);
}

static int mtnmount_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  int r;
  printf("%s: path=%s size=%u offset=%llu\n", __func__, path, size, offset);
  r = mtn_read((int)(fi->fh), buf, size, offset); 
  return r;
}

static int mtnmount_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  int r;
  printf("%s: path=%s size=%u offset=%llu\n", __func__, path, size, offset);
  r = mtn_write((int)(fi->fh), buf, size, offset); 
  return r;
}

static struct fuse_operations mtn_oper = {
  .getattr  = mtnmount_getattr,
  .readlink = mtnmount_readlink,
  .readdir  = mtnmount_readdir,
  .create   = mtnmount_create,
  .open     = mtnmount_open,
  .read     = mtnmount_read,
  .release  = mtnmount_release,
};

int main(int argc, char *argv[])
{
  mtn_init_option();
  mtn_hello();
  return fuse_main(argc, argv, &mtn_oper, NULL);
}

