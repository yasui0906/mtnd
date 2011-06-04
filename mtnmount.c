#define FUSE_USE_VERSION 28
#include "mtnfs.h"
#include <fuse.h>

kdir *dir_cache = NULL;
pthread_mutex_t mtn_read_mutex  = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mtn_write_mutex = PTHREAD_MUTEX_INITIALIZER;

void clear_dircache(const char *path){
  kdir *kd;
  char b[PATH_MAX];
  char d[PATH_MAX];
  strcpy(b, path);
  strcpy(d, dirname(b));
  for(kd=dir_cache;kd;kd=kd->next){
    if(strcmp(kd->path, d) == 0){
      if(dir_cache == kd){
        dir_cache = kd->next;
      }
      rmkdir(kd);
      break;
    }
  }
}

static int mtnmount_getattr(const char *path, struct stat *stbuf)
{
  int   res = 0;
  char  d[PATH_MAX];
  char  f[PATH_MAX];
  kdir  *kd  = NULL;
  kstat *krt = NULL;
  kstat *kst = NULL;

  dirbase(path, d, f);
  lprintf(0, "%s: dir=%s file=%s\n", __func__, d, f);
  memset(stbuf, 0, sizeof(struct stat));
  if(strcmp(path, "/") == 0) {
    stbuf->st_mode  = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
    return(0);
  }
  for(kd=dir_cache;kd;kd=kd->next){
    if(strcmp(kd->path, d) == 0){
      krt = kd->kst;
      break;
    }
  }
  if(krt == NULL){
    krt = mtn_list(path);
  }
  for(kst=krt;kst;kst=kst->next){  
    if(strcmp(kst->name, f) == 0){
      memcpy(stbuf, &(kst->stat), sizeof(struct stat));
      return(0);
    }
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
  kstat *krt = mtn_list(path);
  kstat *kst = krt;

  mtn_hello();
  filler(buf, ".",  NULL, 0);
  filler(buf, "..", NULL, 0);
  while(kst){
    lprintf(0, "%s: name=%s\n", __func__, kst->name);
    filler(buf, kst->name, NULL, 0);
    kst = kst->next;
  }
  dir_cache = mkkdir(path, krt, dir_cache);
  return 0;
}

static int mtnmount_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  int  r = 0;
  char d[PATH_MAX];
  char f[PATH_MAX];

  lprintf(0, "%s: 1 path=%s fh=%llu mode=%o\n", __func__, path, fi->fh, mode);
  dirbase(path,d,f);
  clear_dircache(d);
  r = mtn_open(path, fi->flags, mode);
  if(r == -1){
    return(-errno);
  }
  fi->fh = (uint64_t)r;
  lprintf(0, "%s: 2 path=%s fh=%llu\n", __func__, path, fi->fh);
  return(0); 
}

static int mtnmount_open(const char *path, struct fuse_file_info *fi)
{
  int  r = 0;
  char d[PATH_MAX];
  char f[PATH_MAX];
  lprintf(0, "%s: 1 path=%s fh=%llu\n", __func__, path, fi->fh);
  dirbase(path,d,f);
  clear_dircache(d);
  r = mtn_open(path, fi->flags, 0);
  if(r == -1){
    return(-errno);
  }
  fi->fh = (uint64_t)r;
  lprintf(0, "%s: 2 path=%s fh=%llu\n", __func__, path, fi->fh);
  return(0); 
}

static int mtnmount_truncate(const char *path, off_t offset)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  clear_dircache(path);
  memset(&kt, 0, sizeof(kt));
  strcpy(kt.path, path);
  kt.type   = MTNCMD_TRUNCATE;
  kt.create = 1;
  mtn_set_string(path, &(kt.send));
  mtn_set_int(&offset, &(kt.send), sizeof(offset));
  if(mtn_callcmd(&kt) == -1){
    return(-errno);
  }
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

static int mtnmount_mkdir(const char *path, mode_t mode)
{
  ktask kt;
  char d[PATH_MAX];
  char f[PATH_MAX];
  lprintf(0, "%s: path=%s\n", __func__, path);
  memset(&kt, 0, sizeof(kt));
  dirbase(path, d, f);
  strcpy(kt.path, path);
  kt.type = MTNCMD_MKDIR;
  mtn_set_string(path, &(kt.send));
  if(mtn_callcmd(&kt) == -1){
    return(-errno);
  }
  clear_dircache(path);
  return(0);
}

static int mtnmount_unlink(const char *path)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  memset(&kt, 0, sizeof(kt));
  strcpy(kt.path, path);
  kt.type = MTNCMD_UNLINK;
  mtn_set_string(path, &(kt.send));
  if(mtn_callcmd(&kt) == -1){
    return(-errno);
  }
  clear_dircache(path);
  return(0);
}

static int mtnmount_rmdir(const char *path)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  memset(&kt, 0, sizeof(kt));
  strcpy(kt.path, path);
  kt.type = MTNCMD_RMDIR;
  mtn_set_string(path, &(kt.send));
  if(mtn_callcmd(&kt) == -1){
    return(-errno);
  }
  clear_dircache(path);
  return(0);
}

static int mtnmount_rename(const char *old_path, const char *new_path)
{
  ktask kt;
  lprintf(0, "%s: old=%s new=%s\n", __func__, old_path, new_path);
  return(0);
}

static int mtnmount_chmod(const char *path, mode_t mode)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  clear_dircache(path);
  return(0);
}

static int mtnmount_chown(const char *path, uid_t uid, gid_t gid)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_statfs(const char *path, struct statvfs *sv)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_flush(const char *path, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_fsync(const char *path, int sync, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "%s: path=%s sync=%d\n", __func__, path, sync);
  return(0);
}

static int mtnmount_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
  ktask kt;
  lprintf(0, "%s: path=%s flags=%d\n", __func__, path, flags);
  return(0);
}

static int mtnmount_getxattr(const char *path, const char *name, char *value, size_t size)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_listxattr(const char *path, char *list, size_t size)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_removexattr(const char *path, const char *name)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_opendir(const char *path, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_releasedir(const char *path, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_fsyncdir (const char *path, int flags, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static void *mtnmount_init(struct fuse_conn_info *conn)
{
  lprintf(0, "%s:\n", __func__);
  return(NULL);
}

static void mtnmount_destroy(void *buff)
{
  lprintf(0, "%s:\n", __func__);
}

static int mtnmount_access (const char *path, int mode)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_fgetattr(const char *path, struct stat *st, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "%s: path=%s fh=%d\n", __func__, path, fi->fh);
  if(fi->fh == 0){
    lprintf(0, "%s: 1\n", __func__);
    return(-EBADF);
  }
  memset(&kt, 0, sizeof(kt));
  strcpy(kt.path, path);
  kt.type = MTNCMD_GETATTR;
  kt.con  = fi->fh;
  mtn_set_string(path, &(kt.send));
  if(mtn_callcmd(&kt) == -1){
    lprintf(0, "%s: 2\n", __func__);
    return(-errno);
  }
  if(mtn_get_stat(st, &(kt.recv)) == -1){
    lprintf(0, "%s: 3\n", __func__);
    return(-EACCES);
  }
  lprintf(0, "%s: 4\n", __func__);
  return(0);
}

static int mtnmount_lock(const char *path, struct fuse_file_info *fi, int cmd, struct flock *l)
{
  ktask kt;
  lprintf(0, "%s: path=%s cmd=%d\n", __func__, path, cmd);
  return(0);
}

static int mtnmount_utimens(const char *path, const struct timespec tv[2])
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_bmap(const char *path, size_t blocksize, uint64_t *idx)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_ioctl(const char *path, int cmd, void *arg, struct fuse_file_info *fi, unsigned int flags, void *data)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_poll(const char *path, struct fuse_file_info *fi, struct fuse_pollhandle *ph, unsigned *reventsp)
{
  ktask kt;
  lprintf(0, "%s: path=%s\n", __func__, path);
  return(0);
}

static struct fuse_operations mtn_oper = {
  .getattr     = mtnmount_getattr,
  .readlink    = mtnmount_readlink,
  .readdir     = mtnmount_readdir,
  .create      = mtnmount_create,
  .open        = mtnmount_open,
  .read        = mtnmount_read,
  .write       = mtnmount_write,
  .release     = mtnmount_release,
  .unlink      = mtnmount_unlink,
  .rmdir       = mtnmount_rmdir,
  .mkdir       = mtnmount_mkdir,
  .rename      = mtnmount_rename,
  .chmod       = mtnmount_chmod,
  .chown       = mtnmount_chown,
  .truncate    = mtnmount_truncate,
  .statfs      = mtnmount_statfs,
  //.flush       = mtnmount_flush,
  //.fsync       = mtnmount_fsync,
  //.setxattr    = mtnmount_setxattr,
  //.getxattr    = mtnmount_getxattr,
  //.listxattr   = mtnmount_listxattr,
  //.removexattr = mtnmount_removexattr,
  .opendir     = mtnmount_opendir,
  .releasedir  = mtnmount_releasedir,
  .fsyncdir    = mtnmount_fsyncdir,
  .init        = mtnmount_init,
  .destroy     = mtnmount_destroy,
  .access      = mtnmount_access,
  .ftruncate   = mtnmount_ftruncate,
  .fgetattr    = mtnmount_fgetattr,
  //.lock        = mtnmount_lock,
  //.utimens     = mtnmount_utimens,
  //.bmap        = mtnmount_bmap,
  //.ioctl       = mtnmount_ioctl,
  //.poll        = mtnmount_poll,
};

int main(int argc, char *argv[])
{
  mtn_init_option();
  mtn_hello();
  pthread_mutex_init(&mtn_read_mutex, NULL);
  pthread_mutex_init(&mtn_write_mutex, NULL);
  return fuse_main(argc, argv, &mtn_oper, NULL);
}
