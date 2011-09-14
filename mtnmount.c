#define FUSE_USE_VERSION 28
#include "mtnfs.h"
#include "common.h"
#include <fuse.h>
pthread_mutex_t *mtn_fmutex;

static int mtnmount_getattr(const char *path, struct stat *stbuf)
{
  kstat *krt = NULL;
  kstat *kst = NULL;
  struct timeval tv;
  char  d[PATH_MAX];
  char  f[PATH_MAX];

  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  memset(stbuf, 0, sizeof(struct stat));
  if(strcmp(path, "/") == 0) {
    stbuf->st_mode  = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
    return(0);
  }
  if(strcmp(path, kopt.mtnstatus_path) == 0) {
    stbuf->st_mode  = S_IFDIR | 0555;
    stbuf->st_nlink = 2;
    return(0);
  }
  if(is_mtnstatus(path, f)){
    gettimeofday(&tv, NULL);
    stbuf->st_mode  = S_IFREG | 0444;
    stbuf->st_atime = tv.tv_sec;
    stbuf->st_mtime = tv.tv_sec;
    stbuf->st_ctime = tv.tv_sec;
    if(strcmp(f, "members") == 0) {
      stbuf->st_size = mtnstatus_members();
      return(0);
    }
    if(strcmp(f, "debuginfo") == 0) {
      stbuf->st_size = mtnstatus_debuginfo();
      return(0);
    }
    if(strcmp(f, "debuglevel") == 0) {
      stbuf->st_mode = S_IFREG | 0644;
      stbuf->st_size = mtnstatus_debuglevel();
      return(0);
    }
    return(-ENOENT);
  }

  dirbase(path,d,f);
  krt = get_dircache(d, 0);
  for(kst=krt;kst;kst=kst->next){
    if(strcmp(kst->name, f) == 0){
      memcpy(stbuf, &(kst->stat), sizeof(struct stat));
      break;
    }
  }
  if(!kst){
    if(kst = mtn_stat(path)){
      addstat_dircache(d, kst);
      memcpy(stbuf, &(kst->stat), sizeof(struct stat));
    }
  }
  delstats(krt);
  return(kst ? 0 : -ENOENT);
}

static int mtnmount_fgetattr(const char *path, struct stat *st, struct fuse_file_info *fi)
{
  int r;
  ktask kt;
  struct timeval tv;
  char  d[PATH_MAX];
  char  f[PATH_MAX];
  lprintf(8, "[debug] %s: path=%s fh=%d\n", __func__, path, fi->fh);
  if(strcmp(path, "/") == 0) {
    st->st_mode  = S_IFDIR | 0755;
    st->st_nlink = 2;
    return(0);
  }
  if(strcmp(path, kopt.mtnstatus_path) == 0){
    st->st_mode  = S_IFDIR | 0555;
    st->st_nlink = 2;
    return(0);
  }
  if(is_mtnstatus(path, f)){
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
      if(strcmp(f, "debuglevel") == 0) {
        st->st_mode  = S_IFREG | 0644;
        return(0);
      }
    }
    return(-EBADF);
  }
  if(fi->fh == 0){
    lprintf(0, "[error] %s: \n", __func__);
    return(-EBADF);
  }
  dirbase(path, d, f);
  memset(&kt, 0, sizeof(kt));
  strcpy(kt.path, path);
  kt.type = MTNCMD_GETATTR;
  kt.con  = fi->fh;
  mtn_set_string((uint8_t *)path, &(kt.send));
  pthread_mutex_lock(&(mtn_fmutex[fi->fh]));
  r = mtn_callcmd(&kt);
  pthread_mutex_unlock(&(mtn_fmutex[fi->fh]));
  if(r == -1){
    lprintf(0, "[error] %s: mtn_callcmd %s\n", __func__, strerror(errno));
    return(-errno);
  }
  if(mtn_get_stat(st, &(kt.recv)) == -1){
    lprintf(0, "[error] %s: mtn_get_stat %s\n", __func__, strerror(errno));
    return(-EACCES);
  }
  return(0);
}

static int mtnmount_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
  kstat *krt = NULL;
  kstat *kst = NULL;
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  filler(buf, ".",  NULL, 0);
  filler(buf, "..", NULL, 0);
  if(strcmp("/", path) == 0){
    filler(buf, kopt.mtnstatus_name, NULL, 0);
  }
  if(strcmp(path, kopt.mtnstatus_path) == 0){
    filler(buf, "members",    NULL, 0);
    filler(buf, "debuginfo",  NULL, 0);
    filler(buf, "debuglevel", NULL, 0);
  }else{
    krt = get_dircache(path,1);
    if(krt == NULL){
      krt = mtn_list(path);
      setstat_dircache(path, krt);
    }
    for(kst=krt;kst;kst=kst->next){
      filler(buf, kst->name, NULL, 0);
    }
    delstats(krt);
  }
  return(0);
}

static int mtnmount_mkdir(const char *path, mode_t mode)
{
  int r;
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  r = mtn_mkdir(path);
  return(r);
}

static int mtnmount_unlink(const char *path)
{
  int  r;
  char d[PATH_MAX];
  dirbase(path, d, NULL);
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  r = mtn_rm(path);
  setstat_dircache(d, NULL);
  return(r);
}

static int mtnmount_rmdir(const char *path)
{
  int  r;
  char d[PATH_MAX];
  dirbase(path, d, NULL);
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  r = mtn_rm(path);
  setstat_dircache(d, NULL);
  return(r);
}

static int mtnmount_symlink(const char *oldpath, const char *newpath)
{
  int  r = 0;
  char d[PATH_MAX];
  dirbase(newpath, d, NULL);
  lprintf(8, "[debug] %s: oldpath=%s newpath=%s\n", __func__, oldpath, newpath);
  r = mtn_symlink(oldpath, newpath);
  setstat_dircache(d, NULL);
  return(r);
}

static int mtnmount_readlink(const char *path, char *buff, size_t size)
{
  int r;
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  r = mtn_readlink(path, buff, size);
  return(r);
}

static int mtnmount_rename(const char *old_path, const char *new_path)
{
  int  r;
  char d0[PATH_MAX];
  char d1[PATH_MAX];
  dirbase(old_path, d0, NULL);
  dirbase(new_path, d1, NULL);
  lprintf(8, "[debug] %s: old_path=%s new_path=%s\n", __func__, old_path, new_path);
  r = mtn_rename(old_path, new_path);
  setstat_dircache(d0, NULL);
  setstat_dircache(d1, NULL);
  return(r);
}

static int mtnmount_chmod(const char *path, mode_t mode)
{
  char d[PATH_MAX];
  dirbase(path, d, NULL);
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  mtn_chmod(path, mode);
  setstat_dircache(d, NULL);
  return(0);
}

static int mtnmount_chown(const char *path, uid_t uid, gid_t gid)
{
  char d[PATH_MAX];
  dirbase(path, d, NULL);
  lprintf(8, "[debug] %s: path=%s uid=%d gid=%d\n", __func__, path, uid, gid);
  mtn_chown(path, uid, gid);
  setstat_dircache(d, NULL);
  return(0);
}

static int mtnmount_utimens(const char *path, const struct timespec tv[2])
{
  int  r;
  char d[PATH_MAX];
  dirbase(path, d, NULL);
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  r = mtn_utime(path, tv[0].tv_sec, tv[1].tv_sec);
  setstat_dircache(d, NULL);
  return(r);
}

static int mtnmount_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  int r = 0;
  lprintf(8, "[debug] %s: path=%s fh=%llu mode=%o\n", __func__, path, fi->fh, mode);
  if(is_mtnstatus(path, NULL)){
    return(-EACCES);
  }
  r = mtn_open(path, fi->flags, mode);
  fi->fh = (uint64_t)r;
  return((r == -1) ? -errno : 0);
}

static int mtnmount_open(const char *path, struct fuse_file_info *fi)
{
  int  r = 0;
  char f[PATH_MAX];
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  if(is_mtnstatus(path, f)){
    if(strcmp("members", f) == 0){
      fi->fh = (uint64_t)get_mtnstatus_members();
    }else if(strcmp("debuginfo", f) == 0){
      fi->fh = (uint64_t)get_mtnstatus_debuginfo();
    }else if(strcmp("debuglevel", f) == 0){
      fi->fh = (uint64_t)get_mtnstatus_debuglevel();
    }else{
      return(-ENOENT);
    }
    return(0);
  }
  r = mtn_open(path, fi->flags, 0);
  fi->fh = (uint64_t)r;
  return((r == -1) ? -errno : 0);
}

static int mtnmount_truncate(const char *path, off_t offset)
{
  ktask kt;
  char f[PATH_MAX];
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  if(is_mtnstatus(path, f)){
    if(strcmp("debuglevel", f) == 0){
      return(0);
    }
    return(-EACCES);
  }
  memset(&kt, 0, sizeof(kt));
  strcpy(kt.path, path);
  kt.type   = MTNCMD_TRUNCATE;
  kt.create = 1;
  mtn_set_string((uint8_t *)path, &(kt.send));
  mtn_set_int(&offset, &(kt.send), sizeof(offset));
  return((mtn_callcmd(&kt) == -1) ? -errno : 0);
}

static int mtnmount_release(const char *path, struct fuse_file_info *fi)
{
  int  r = 0;
  char f[PATH_MAX];
  lprintf(8, "[debug] %s: path=%s fh=%llu\n", __func__, path, fi->fh);
  if(is_mtnstatus(path, f)){
    if(strcmp("members", f) == 0){
      xfree((void *)(fi->fh));
      fi->fh = 0;
    }else if(strcmp("debuginfo", f) == 0){
      xfree((void *)(fi->fh));
      fi->fh = 0;
    }else if(strcmp("debuglevel", f) == 0){
      set_debuglevel(atoi((char *)(fi->fh)));
      xfree((void *)(fi->fh));
      fi->fh = 0;
    }else{
      r = -EBADF;
    }
  }else{
    if(fi->fh){
      if(mtn_close(fi->fh) != 0){
        r = -errno;
      }
      fi->fh = 0;
    }
  }
  return(r);
}

static int mtnmount_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  int r;
  int l;
  if(offset == 0){
    lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  }
  if(is_mtnstatus(path, NULL)){
    l = strlen((void *)(fi->fh));
    if(offset > l){
      l = 0;
    }else{
      l -= offset;
    }
    r = (size > l) ? l : size;
    if(r){
      memcpy(buf, (char *)(fi->fh) + offset, r);
    }
  }else{
    pthread_mutex_lock(&(mtn_fmutex[fi->fh]));
    r = mtn_read((int)(fi->fh), buf, size, offset);
    pthread_mutex_unlock(&(mtn_fmutex[fi->fh]));
  }
  return r;
}

static int mtnmount_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  int r;
  if(offset == 0){
    lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  }
  if(is_mtnstatus(path, NULL)){
    fi->fh = (uint64_t)xrealloc((void *)(fi->fh), size);
    memcpy((void *)(fi->fh), buf, size);
    return(size); 
  }
  pthread_mutex_lock(&(mtn_fmutex[fi->fh]));
  r = mtn_write((int)(fi->fh), buf, size, offset); 
  pthread_mutex_unlock(&(mtn_fmutex[fi->fh]));
  return r;
}

static int mtnmount_statfs(const char *path, struct statvfs *sv)
{
  kmember *m;
  kmember *km = mtn_info();
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  statvfs("/", sv);
  sv->f_blocks = 0;
  sv->f_bfree  = 0;
  sv->f_bavail = 0;
  for(m=km;m;m=m->next){
    sv->f_blocks += (m->dsize * m->fsize) / sv->f_frsize;
    sv->f_bfree  += ((m->dfree * m->bsize) - m->limit) / sv->f_bsize;
    sv->f_bavail += ((m->dfree * m->bsize) - m->limit) / sv->f_bsize;
  }
  del_members(km);
  return(0);
}

static int mtnmount_opendir(const char *path, struct fuse_file_info *fi)
{
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_releasedir(const char *path, struct fuse_file_info *fi)
{
  lprintf(8, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static void *mtnmount_init(struct fuse_conn_info *conn)
{
  lprintf(0, "========================\n");
  lprintf(0, "%s start (ver %s)\n", MODULE_NAME, PACKAGE_VERSION);
  lprintf(0, "MulticastIP: %s\n", kopt.mcast_addr);
  lprintf(0, "PortNumber : %d\n", kopt.mcast_port);
  lprintf(0, "DebugLevel : %d\n", kopt.debuglevel);
  mkpidfile();
  return(NULL);
}

static void mtnmount_destroy(void *buff)
{
  rmpidfile();
  lprintf(0, "%s finished\n", MODULE_NAME);
}

//-------------------------------------------------------------------------------------
// 以下未実装
//-------------------------------------------------------------------------------------
static int mtnmount_flush(const char *path, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_fsync(const char *path, int sync, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "[debug] %s: path=%s sync=%d\n", __func__, path, sync);
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

static int mtnmount_fsyncdir (const char *path, int flags, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_access (const char *path, int mode)
{
  ktask kt;
  lprintf(0, "[debug] %s: path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
  ktask kt;
  lprintf(0, "[debug] %s: CALL path=%s\n", __func__, path);
  lprintf(0, "[debug] %s: EXIT path=%s\n", __func__, path);
  return(0);
}

static int mtnmount_lock(const char *path, struct fuse_file_info *fi, int cmd, struct flock *l)
{
  ktask kt;
  lprintf(0, "[debug] %s: path=%s cmd=%d\n", __func__, path, cmd);
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
  .symlink     = mtnmount_symlink,
  .mkdir       = mtnmount_mkdir,
  .rename      = mtnmount_rename,
  .chmod       = mtnmount_chmod,
  .chown       = mtnmount_chown,
  .truncate    = mtnmount_truncate,
  .statfs      = mtnmount_statfs,
  .opendir     = mtnmount_opendir,
  .releasedir  = mtnmount_releasedir,
  .fsyncdir    = mtnmount_fsyncdir,
  .init        = mtnmount_init,
  .destroy     = mtnmount_destroy,
  .ftruncate   = mtnmount_ftruncate,
  .fgetattr    = mtnmount_fgetattr,
  .utimens     = mtnmount_utimens,
  //.access      = mtnmount_access,
  //.flush       = mtnmount_flush,
  //.fsync       = mtnmount_fsync,
  //.setxattr    = mtnmount_setxattr,
  //.getxattr    = mtnmount_getxattr,
  //.listxattr   = mtnmount_listxattr,
  //.removexattr = mtnmount_removexattr,
  //.lock        = mtnmount_lock,
  //.bmap        = mtnmount_bmap,
  //.ioctl       = mtnmount_ioctl,
  //.poll        = mtnmount_poll,
};

#define MTNFS_OPT_KEY(t, p) { t, offsetof(struct cmdoption, p), 0 }
static struct fuse_opt mtnmount_opts[] = {
  MTNFS_OPT_KEY("-m %s",    addr),
  MTNFS_OPT_KEY("-p %s",    port),
  MTNFS_OPT_KEY("-D %s",    debuglevel),
  MTNFS_OPT_KEY("--pid=%s", pid),
  FUSE_OPT_KEY("-h",        1),
  FUSE_OPT_KEY("--help",    1),
  FUSE_OPT_KEY("--version", 2),
  FUSE_OPT_KEY("-f",        3),
  FUSE_OPT_END
};

static int mtnmount_opt_parse(void *data, const char *arg, int key, struct fuse_args *outargs)
{
  struct cmdoption *opts = data;
  if(key == 1){
    fprintf(stderr, "usage: mtnmount mountpoint [options]\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "mtnfs options:\n");
    fprintf(stderr, "    -m IPADDR              Multicast Address(default: 224.0.0.110)\n");
    fprintf(stderr, "    -p PORT                Server Port(default: 6000)\n");
    fprintf(stderr, "    --pid=path             pid file(ex: /var/run/mtnmount.pid)\n");
    fprintf(stderr, "\n");
    return fuse_opt_add_arg(outargs, "-ho");
  }
  if(key == 2){
    fprintf(stderr, "%s version: %s\n", MODULE_NAME, PACKAGE_VERSION);
  }
  if(key == 3){
    kopt.daemonize = 0;
  }
  return(1);
}

int main(int argc, char *argv[])
{
  int i;
  struct cmdoption opts;
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

  mtn_init_option();
  memset(&opts, 0, sizeof(opts));
  fuse_opt_parse(&args, &opts, mtnmount_opts, mtnmount_opt_parse);
  if(opts.port){
    kopt.mcast_port = atoi(opts.port);
  }
  if(opts.addr){
    strcpy(kopt.mcast_addr, opts.addr);
  }
  if(opts.debuglevel){
    kopt.debuglevel = atoi(opts.debuglevel);
  }
  if(opts.pid){
    if(*opts.pid == '/'){
      strcpy(kopt.pid, opts.pid);
    }else{
      sprintf(kopt.pid, "%s/%s", kopt.cwd, opts.pid);
    }
  }
  mtn_fmutex = xmalloc(sizeof(pthread_mutex_t) * MTN_OPENLIMIT);
  for(i=0;i<MTN_OPENLIMIT;i++){
    pthread_mutex_init(&(mtn_fmutex[i]), NULL);
  }
  return fuse_main(args.argc, args.argv, &mtn_oper, NULL);
}

