/*
 * mtnfs.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"
#include "common.h"
typedef void (*MTNFSTASKFUNC)(ktask *);

void version()
{
  lprintf(0, "%s version %s\n", MODULE_NAME, PACKAGE_VERSION);
}

void usage()
{
  printf("%s version %s\n", MODULE_NAME, PACKAGE_VERSION);
  printf("usage: %s [OPTION]\n", MODULE_NAME);
  printf("\n");
  printf("  OPTION\n");
  printf("   -h         # help\n");
  printf("   -v         # version\n");
  printf("   -n         # no daemon\n");
  printf("   -e dir     # export dir\n");
  printf("   -m addr    # mcast addr\n");
  printf("   -p port    # TCP/UDP port\n");
  printf("   -D num     # debug level\n");
  printf("   -l size    # limit size (MB)\n");
  printf("   --pid=path # pid file(ex: /var/run/mtnfs.pid)\n");
  printf("\n");
}

int get_diskfree(uint32_t *bsize, uint32_t *fsize, uint64_t *dsize, uint64_t *dfree)
{
  uint64_t size = 0;
  struct statvfs vf;

  if(statvfs(".", &vf) == -1){
    lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  *bsize = vf.f_bsize;
  *fsize = vf.f_frsize;
  *dsize = vf.f_blocks;
  *dfree = vf.f_bfree;
  return(0);
}

uint64_t get_datasize(char *path)
{
  char full[PATH_MAX];
  off_t size = 0;
  DIR *d = opendir(path);
  struct dirent *ent;
  struct stat st;

  while(ent = readdir(d)){
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

void start_message(const char *msg)
{
  uint32_t bsize;
  uint32_t fsize;
  uint64_t dsize;
  uint64_t dfree;
  get_diskfree(&bsize, &fsize, &dsize, &dfree);
  lprintf(0, "%s", msg);
  version();
  lprintf(0, "pid  : %d\n", getpid());
  lprintf(0, "host : %s\n", kopt.host);
  lprintf(0, "base : %s\n", kopt.cwd);
  lprintf(0, "size : %llu [KB]\n", fsize * dsize / 1024);
  lprintf(0, "free : %llu [KB]\n", bsize * dfree / 1024);
  lprintf(0, "limit: %llu [KB]\n", kopt.free_limit/1024);
}

char *mtnfs_fix_path(char *path){
  char buff[PATH_MAX];
  strcpy(buff, path);
  if(buff[0] == '/'){
    strcpy(path, buff + 1);
    return mtnfs_fix_path(path);
  }
  if(memcmp(buff, "./", 2) == 0){
    strcpy(path, buff + 2);
    return mtnfs_fix_path(path);
  }
  if(memcmp(buff, "../", 3) == 0){
    strcpy(path, buff + 3);
    return mtnfs_fix_path(path);
  }
  return(path);
}

ktask *mtnfs_task_create(kdata *data, kaddr *addr)
{
  ktask *kt = malloc(sizeof(ktask));
  memset(kt, 0, sizeof(ktask));
  memcpy(&(kt->addr), addr, sizeof(kaddr));
  memcpy(&(kt->recv), data, sizeof(kdata));
  kt->type = data->head.type;
  if(kopt.task){
    kopt.task->prev = kt;
  }
  kt->next  = kopt.task;
  kopt.task = kt;
  return(kt);
}

ktask *mtnfs_task_delete(ktask *task)
{
  ktask *pt = NULL;
  ktask *nt = NULL;
  if(!task){
    return(NULL);
  }
  pt = task->prev;
  nt = task->next;
  if(pt){
    pt->next = nt;
  }
  if(nt){
    nt->prev = pt;
  }
  if(kopt.task == task){
    kopt.task = nt;
  }
  free(task);
  return(nt);
}

//-------------------------------------------------------------------
// UDP PROSESS
//-------------------------------------------------------------------
static void mtnfs_hello_process(ktask *kt)
{
  //lprintf(1, "[debug] %s: START\n", __func__);
  mtn_set_string(kopt.host, &(kt->send));
  kt->fin = 1;
  //lprintf(1, "[debug] %s: END\n", __func__);
}

static void mtnfs_info_process(ktask *kt)
{
  uint32_t bsize;
  uint32_t fsize;
  uint64_t dsize;
  uint64_t dfree;
  uint64_t limit;
  limit = kopt.free_limit;
  get_diskfree(&bsize, &fsize, &dsize, &dfree);
  kt->send.head.fin = 1;
  mtn_set_int(&bsize, &(kt->send), sizeof(bsize));
  mtn_set_int(&fsize, &(kt->send), sizeof(fsize));
  mtn_set_int(&dsize, &(kt->send), sizeof(dsize));
  mtn_set_int(&dfree, &(kt->send), sizeof(dfree));
  mtn_set_int(&limit, &(kt->send), sizeof(limit));
  kt->fin = 1;
}

int mtnfs_list_dir(ktask *kt)
{
  uint16_t len;
  struct  dirent *ent;
  char full[PATH_MAX];
  while(ent = readdir(kt->dir)){
    if(ent->d_name[0] == '.'){
      continue;
    }
    sprintf(full, "%s/%s", kt->path, ent->d_name);
    if(lstat(full, &(kt->stat)) == 0){
      len  = mtn_set_string(ent->d_name, NULL);
      len += mtn_set_stat(&(kt->stat),   NULL);
      if(kt->send.head.size + len <= kopt.max_packet_size){
        mtn_set_string(ent->d_name, &(kt->send));
        mtn_set_stat(&(kt->stat),   &(kt->send));
      }else{
        send_dgram(kt->con, &(kt->send), &(kt->addr));
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

static void mtnfs_list_process(ktask *kt)
{
  //lprintf(1, "[debug] %s: START\n", __func__);
  char buff[PATH_MAX];
  if(kt->dir){
    mtnfs_list_dir(kt);
  }else{
    kt->fin = 1;
    kt->send.head.size = 0;
    mtn_get_string(buff, &(kt->recv));
    mtnfs_fix_path(buff);
    sprintf(kt->path, "./%s", buff);
    if(lstat(kt->path, &(kt->stat)) == -1){
      if(errno != ENOENT){
        lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
      }
      kt->send.head.fin = 1;
    }
    if(S_ISREG(kt->stat.st_mode)){
      mtn_set_string(basename(kt->path), &(kt->send));
      mtn_set_stat(&(kt->stat), &(kt->send));
      kt->send.head.fin = 1;
    }
    if(S_ISDIR(kt->stat.st_mode)){
      if(kt->dir = opendir(kt->path)){
        kt->fin = 0;
      }else{
        lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }
  //lprintf(1, "[debug] %s: END\n", __func__);
}

static void mtnfs_stat_process(ktask *kt)
{
  //lprintf(1, "[debug] %s: START\n", __func__);
  char buff[PATH_MAX];
  char file[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(buff, &(kt->recv));
  mtnfs_fix_path(buff);
  sprintf(kt->path, "./%s", buff);
  if(lstat(kt->path, &(kt->stat)) == -1){
    if(errno != ENOENT){
      lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
      kt->send.head.type = MTNRES_ERROR;
    }
  }else{
    kt->send.head.type = MTNRES_SUCCESS;
    dirbase(kt->path, NULL, file);
    mtn_set_string(file, &(kt->send));
    mtn_set_stat(&(kt->stat), &(kt->send));
  }
  //lprintf(1,"[debug] %s: END\n", __func__);
}

static void mtnfs_mkdir_process(ktask *kt)
{
  lprintf(1, "[debug] %s: CALL\n", __func__);
  char buff[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(buff, &(kt->recv));
  mtnfs_fix_path(buff);
  sprintf(kt->path, "./%s", buff);
  kt->send.head.type = MTNRES_SUCCESS;
  if(mkdir_ex(kt->path) == -1){
    lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(1, "[debug] %s: EXIT\n", __func__);
}

static void mtnfs_rm_process(ktask *kt)
{
  lprintf(1, "[debug] %s: CALL\n", __func__);
  char buff[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(buff, &(kt->recv));
  mtnfs_fix_path(buff);
  sprintf(kt->path, "./%s", buff);
  kt->send.head.type = MTNRES_SUCCESS;
  if(lstat(kt->path, &(kt->stat)) == -1){
    lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }else{
    if(S_ISDIR(kt->stat.st_mode)){
      rmdir(kt->path);
    }else{
      unlink(kt->path);
    }
  }
  lprintf(1, "[debug] %s: EXIT\n", __func__);
}

static void mtnfs_rename_process(ktask *kt)
{
  lprintf(1, "[debug] %s: CALL\n", __func__);
  char obuff[PATH_MAX];
  char nbuff[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(obuff, &(kt->recv));
  mtn_get_string(nbuff, &(kt->recv));
  mtnfs_fix_path(obuff);
  mtnfs_fix_path(nbuff);
  sprintf(kt->path, "./%s", obuff);
  kt->send.head.type = MTNRES_SUCCESS;
  if(rename(obuff, nbuff) == -1){
    lprintf(0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), obuff, nbuff);
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(1, "[debug] %s: EXIT\n", __func__);
}

static void mtnfs_symlink_process(ktask *kt)
{
  lprintf(1, "[debug] %s: CALL\n", __func__);
  char oldpath[PATH_MAX];
  char newpath[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(oldpath, &(kt->recv));
  mtn_get_string(newpath, &(kt->recv));
  mtnfs_fix_path(newpath);
  kt->send.head.type = MTNRES_SUCCESS;
  if(symlink(oldpath, newpath) == -1){
    lprintf(0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), oldpath, newpath);
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(1, "[debug] %s: EXIT o=%s n=%s\n", __func__, oldpath, newpath);
}

static void mtnfs_readlink_process(ktask *kt)
{
  lprintf(1, "[debug] %s: CALL\n", __func__);
  ssize_t size;
  char newpath[PATH_MAX];
  char oldpath[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(newpath, &(kt->recv));
  mtnfs_fix_path(newpath);
  kt->send.head.type = MTNRES_SUCCESS;
  size = readlink(newpath, oldpath, PATH_MAX);
  if(size == -1){
    lprintf(0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), oldpath, newpath);
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }else{
    oldpath[size] = 0;
    mtn_set_string(oldpath, &(kt->send));
  }
  lprintf(1, "[debug] %s: EXIT o=%s n=%s\n", __func__, oldpath, newpath);
}

static void mtnfs_chmod_process(ktask *kt)
{
  mode_t mode;
  char path[PATH_MAX];
  lprintf(0,"[debug] %s: CALL\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(path, &(kt->recv));
  mtn_get_int(&mode, &(kt->recv), sizeof(mode));
  mtnfs_fix_path(path);
  kt->send.head.type = MTNRES_SUCCESS;
  if(chmod(path, mode) == -1){
    lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(0,"[debug] %s: EXIT\n", __func__);
}

static void mtnfs_chown_process(ktask *kt)
{
  uid_t uid;
  gid_t gid;
  char path[PATH_MAX];
  lprintf(0,"[debug] %s: CALL\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(path, &(kt->recv));
  mtn_get_int(&uid, &(kt->recv), sizeof(uid));
  mtn_get_int(&gid, &(kt->recv), sizeof(gid));
  mtnfs_fix_path(path);
  kt->send.head.type = MTNRES_SUCCESS;
  if(chown(path, uid, gid) == -1){
    lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(0,"[debug] %s: EXIT\n", __func__);
}

static void mtnfs_utime_process(ktask *kt)
{
  struct utimbuf ut;
  char path[PATH_MAX];
  lprintf(0,"[debug] %s: CALL\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(path, &(kt->recv));
  mtn_get_int(&(ut.actime),  &(kt->recv), sizeof(ut.actime));
  mtn_get_int(&(ut.modtime), &(kt->recv), sizeof(ut.modtime));
  mtnfs_fix_path(path);
  kt->send.head.type = MTNRES_SUCCESS;
  if(utime(path, &ut) == -1){
    lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(0,"[debug] %s: EXIT\n", __func__);
}

void mtnfs_udp_process(int s)
{
  //lprintf(1, "[debug] %s: START\n", __func__);
  kaddr addr;
  kdata data;
  addr.len = sizeof(addr.addr);
  if(recv_dgram(s, &data, &(addr.addr.addr), &(addr.len)) == -1){
    return;
  }
  mtnfs_task_create(&data, &addr);
  //lprintf(1, "[debug] %s: END\n", __func__);
}

void mtnfs_task_process(int s)
{
  //lprintf(1, "[debug] %s: START\n", __func__);
  ktask *kt = kopt.task;
  MTNFSTASKFUNC taskfunc[MTNCMD_MAX];
  memset(taskfunc, 0, sizeof(taskfunc));
  taskfunc[MTNCMD_HELLO]    = (MTNFSTASKFUNC)mtnfs_hello_process;
  taskfunc[MTNCMD_INFO]     = (MTNFSTASKFUNC)mtnfs_info_process;
  taskfunc[MTNCMD_LIST]     = (MTNFSTASKFUNC)mtnfs_list_process;
  taskfunc[MTNCMD_STAT]     = (MTNFSTASKFUNC)mtnfs_stat_process;
  taskfunc[MTNCMD_MKDIR]    = (MTNFSTASKFUNC)mtnfs_mkdir_process;
  taskfunc[MTNCMD_RMDIR]    = (MTNFSTASKFUNC)mtnfs_rm_process;
  taskfunc[MTNCMD_UNLINK]   = (MTNFSTASKFUNC)mtnfs_rm_process;
  taskfunc[MTNCMD_RENAME]   = (MTNFSTASKFUNC)mtnfs_rename_process;
  taskfunc[MTNCMD_SYMLINK]  = (MTNFSTASKFUNC)mtnfs_symlink_process;
  taskfunc[MTNCMD_READLINK] = (MTNFSTASKFUNC)mtnfs_readlink_process;
  taskfunc[MTNCMD_CHMOD]    = (MTNFSTASKFUNC)mtnfs_chmod_process;
  taskfunc[MTNCMD_CHOWN]    = (MTNFSTASKFUNC)mtnfs_chown_process;
  taskfunc[MTNCMD_UTIME]    = (MTNFSTASKFUNC)mtnfs_utime_process;
  while(kt){
    kt->con = s;
    MTNFSTASKFUNC task = taskfunc[kt->type];
    if(task){
      task(kt);
    }else{
      kt->fin = 1;
      kt->send.head.fin  = 1;
      kt->send.head.size = 0;
      kt->send.head.type = MTNRES_ERROR;
      lprintf(0, "[error] %s: Function Not Found type=%d\n", __func__, kt->type);
    }
    if(kt->fin){
      send_dgram(kt->con, &(kt->send), &(kt->addr));
      kt = mtnfs_task_delete(kt);
    }else{
      kt = kt->next;
    }
  }
  //lprintf(1, "[debug] %s: END\n", __func__);
}

//------------------------------------------------------
// mtntool commands for TCP
//------------------------------------------------------
void mtnfs_child_get(ktask *kt)
{
  lprintf(1, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  lprintf(0,"[info]  %s: PATH=%s\n", __func__, kt->path);
  kt->fd = open(kt->path, O_RDONLY);
  if(kt->fd == -1){
    lprintf(0,"[error] %s: file open error %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }else{
    while(is_loop){
      kt->send.head.size = read(kt->fd, kt->send.data.data, sizeof(kt->send.data.data));
      if(kt->send.head.size == 0){ // EOF
        close(kt->fd);
        kt->send.head.fin = 1;
        kt->fd  = 0;
        kt->fin = 1;
        break;
      }
      if(kt->send.head.size == -1){
        lprintf(0,"[error] %s: file read error %s %s\n", __func__, strerror(errno), kt->path);
        kt->send.head.type = MTNRES_ERROR;
        kt->send.head.size = 0;
        kt->send.head.fin  = 1;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
        kt->fin = 1;
        break;
      }
      if(send_data_stream(kt->con, &(kt->send)) == -1){
        lprintf(0,"[error] %s: send error %s %s\n", __func__, strerror(errno), kt->path);
        kt->send.head.type = MTNRES_ERROR;
        kt->send.head.size = 0;
        kt->send.head.fin  = 1;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
        kt->fin = 1;
        break;
      }
    }
  }
  lprintf(1, "[debug] %s: END\n", __func__);
}

void mtnfs_child_set(ktask *kt)
{
  char d[PATH_MAX];
  char f[PATH_MAX];

  lprintf(1, "[debug] %s: START\n", __func__);
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
        lprintf(0, "[error] %s: file write error %s\n",__func__, strerror(errno));
        kt->fin = 1;
        kt->send.head.fin = 1;
        kt->send.head.type = MTNRES_ERROR;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }else{
    //----- open/create -----
    mtn_get_string(kt->path,  &(kt->recv));
    mtn_get_stat(&(kt->stat), &(kt->recv));
    lprintf(0,"[info]  %s: creat %s\n", __func__, kt->path);
    dirbase(kt->path, d, f);
    if(mkdir_ex(d) == -1){
      lprintf(0,"[error] %s: mkdir error %s %s\n", __func__, strerror(errno), d);
      kt->fin = 1;
      kt->send.head.fin = 1;
      kt->send.head.type = MTNRES_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      kt->fd = creat(kt->path, kt->stat.st_mode);
      if(kt->fd == -1){
        lprintf(0,"[error] %s: can't creat %s %s\n", __func__, strerror(errno), kt->path);
        kt->fd  = 0;
        kt->fin = 1;
        kt->send.head.fin = 1;
        kt->send.head.type = MTNRES_ERROR;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }
  lprintf(1, "[debug] %s: END\n", __func__);
}

//------------------------------------------------------
// mtnmount commands for TCP
//------------------------------------------------------
void mtnfs_child_open(ktask *kt)
{
  int flags;
  mode_t mode;
  char d[PATH_MAX];
  char f[PATH_MAX];
  mtn_get_string(kt->path, &(kt->recv));
  mtn_get_int(&flags, &(kt->recv), sizeof(flags));
  mtn_get_int(&mode,  &(kt->recv), sizeof(mode));
  mtnfs_fix_path(kt->path);
  dirbase(kt->path, d, f);
  mkdir_ex(d);
  kt->fd = open(kt->path, flags, mode);
  if(kt->fd == -1){
    kt->fd = 0;
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    lprintf(0, "[error] %s: %s, path=%s create=%d mode=%o\n", __func__, strerror(errno), kt->path, ((flags & O_CREAT) != 0), mode);
  }else{
    fstat(kt->fd, &(kt->stat));
    lprintf(1, "[info]  %s: path=%s create=%d mode=%o\n", __func__, kt->path, ((flags & O_CREAT) != 0), mode);
  }
}

void mtnfs_child_read(ktask *kt)
{
  int r;
  size_t  size;
  off_t offset;
  mtn_get_int(&size,   &(kt->recv), sizeof(size));
  mtn_get_int(&offset, &(kt->recv), sizeof(offset));

  lseek(kt->fd, offset, SEEK_SET);
  while(size){
    if(size < MAX_DATASIZE){
      r = read(kt->fd, kt->send.data.data, size);
    }else{
      r = read(kt->fd, kt->send.data.data, MAX_DATASIZE);
    }
    if(r == 0){
      break;
    }
    if(r == -1){
      break;
    }
    size -= r;
    kt->send.head.fin  = 0;
    kt->send.head.size = r;
    send_data_stream(kt->con, &(kt->send));
  }
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
}

void mtnfs_child_write(ktask *kt)
{
  size_t  size;
  void   *buff;
  off_t offset;

  mtn_get_int(&offset, &(kt->recv), sizeof(offset));
  size = kt->recv.head.size;
  buff = kt->recv.data.data;
  lseek(kt->fd, offset, SEEK_SET);
  while(size){
    kt->res = write(kt->fd, buff, size);
    if(kt->res == -1){
      kt->send.head.type = MTNRES_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
      break;
    }
    buff += kt->res;
    size -= kt->res;
  }
}

void mtnfs_child_close(ktask *kt)
{
  if(kt->fd){
    fstat(kt->fd, &(kt->stat));
    close(kt->fd);
    kt->fd = 0;
  }
}

void mtnfs_child_truncate(ktask *kt)
{
  off_t offset;
  mtn_get_string(kt->path, &(kt->recv));
  mtn_get_int(&offset, &(kt->recv), sizeof(offset));
  mtnfs_fix_path(kt->path);
  stat(kt->path, &(kt->stat));
  if(truncate(kt->path, offset) == -1){
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    lprintf(0, "%s: NG path=%s offset=%llu\n", __func__, kt->path, offset);
  }else{
    lprintf(0, "%s: OK path=%s offset=%llu\n", __func__, kt->path, offset);
  }
}

void mtnfs_child_mkdir(ktask *kt)
{
  lprintf(1, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnfs_fix_path(kt->path);
  if(mkdir(kt->path, 0777) == -1){
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(1, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  lprintf(1, "[debug] %s: END\n", __func__);
}

void mtnfs_child_rmdir(ktask *kt)
{
  lprintf(1, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnfs_fix_path(kt->path);
  if(rmdir(kt->path) == -1){
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(1, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  lprintf(1, "[debug] %s: END\n", __func__);
}

void mtnfs_child_unlink(ktask *kt)
{
  lprintf(1, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnfs_fix_path(kt->path);
  if(lstat(kt->path, &(kt->stat)) == -1){
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    lprintf(0,"[debug] %s: stat error: %s %s\n", __func__, strerror(errno), kt->path);
    return;
  }
  if(unlink(kt->path) == -1){
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    lprintf(0,"[debug] %s: unlink error: %s %s\n", __func__, strerror(errno), kt->path);
    return;
  }
  lprintf(1, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  lprintf(1, "[debug] %s: END\n", __func__);
}

void mtnfs_child_getattr(ktask *kt)
{
  lprintf(1, "[debug] %s: START\n", __func__);
  struct stat st;
  if(kt->fd){
    if(fstat(kt->fd, &st) == -1){
      lprintf(0, "[error] %s: 1\n", __func__);
      kt->send.head.type = MTNRES_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      lprintf(1, "[debug] %s: 2\n", __func__);
      mtn_set_stat(&st, &(kt->send));
    }
  }else{
    mtn_get_string(kt->path, &(kt->recv));
    if(lstat(kt->path, &st) == -1){
      lprintf(0, "[error] %s: 3\n", __func__);
      kt->send.head.type = MTNRES_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      lprintf(1, "[debug] %s: 4\n", __func__);
      mtn_set_stat(&st, &(kt->send));
    }
  }
  lprintf(1, "[debug] %s: END\n", __func__);
}

void mtnfs_child_chmod(ktask *kt)
{
  uint32_t mode;
  lprintf(0,"[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtn_get_int32(&mode, &(kt->recv), sizeof(mode));
  if(chmod(kt->path, mode) == -1){
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }else{    
    if(stat(kt->path, &(kt->stat)) == -1){
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtn_set_stat(&(kt->stat), &(kt->send));
    }
  }
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnfs_child_chown(ktask *kt)
{
  uint32_t uid;
  uint32_t gid;
  lprintf(0,"[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtn_get_int32(&uid, &(kt->recv));
  mtn_get_int32(&gid, &(kt->recv));
  if(chown(kt->path, uid, gid) == -1){
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }else{    
    if(stat(kt->path, &(kt->stat)) == -1){
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtn_set_stat(&(kt->stat), &(kt->send));
    }
  }
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnfs_child_setattr(ktask *kt)
{
  lprintf(0,"[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnfs_child_error(ktask *kt)
{
  errno = EACCES;
  lprintf(0, "[error] %s: TYPE=%u\n", __func__, kt->recv.head.type);
  kt->send.head.type = MTNRES_ERROR;
  mtn_set_int(&errno, &(kt->send), sizeof(errno));
}

void mtnfs_child(ktask *kt)
{
  char addr[64];
  MTNFSTASKFUNC call_func;
  MTNFSTASKFUNC func_list[MTNCMD_MAX];
  memset(func_list, 0, sizeof(func_list));
  func_list[MTNCMD_SET]      = mtnfs_child_set;
  func_list[MTNCMD_GET]      = mtnfs_child_get;
  func_list[MTNCMD_OPEN]     = mtnfs_child_open;
  func_list[MTNCMD_READ]     = mtnfs_child_read;
  func_list[MTNCMD_WRITE]    = mtnfs_child_write;
  func_list[MTNCMD_CLOSE]    = mtnfs_child_close;
  func_list[MTNCMD_TRUNCATE] = mtnfs_child_truncate;
  func_list[MTNCMD_MKDIR]    = mtnfs_child_mkdir;
  func_list[MTNCMD_RMDIR]    = mtnfs_child_rmdir;
  func_list[MTNCMD_UNLINK]   = mtnfs_child_unlink;
  func_list[MTNCMD_CHMOD]    = mtnfs_child_chmod;
  func_list[MTNCMD_CHOWN]    = mtnfs_child_chown;
  func_list[MTNCMD_GETATTR]  = mtnfs_child_getattr;
  func_list[MTNCMD_SETATTR]  = mtnfs_child_setattr;

  v4addr(&(kt->addr), addr, sizeof(addr));
  lprintf(1, "[debug] %s: accept from %s:%d\n", __func__, addr, v4port(&(kt->addr)));
  while(is_loop && (kt->fin == 0)){
    kt->send.head.ver  = PROTOCOL_VERSION;
    kt->send.head.type = MTNRES_SUCCESS;
    kt->send.head.size = 0;
    kt->res = recv_data_stream(kt->con, &(kt->recv));
    if(kt->res == -1){
      lprintf(0, "[error] %s: recv error\n", __func__);
      break;
    }
    if(kt->res == 1){
      break;
    }
    //lprintf(0,"[info]  %s: pid=%d recv_type=%d recv_size=%d\n", __func__, getpid(), kt->recv.head.type, kt->recv.head.size);
    if(call_func = func_list[kt->recv.head.type]){
      call_func(kt);
    }else{
      mtnfs_child_error(kt);
    }
    send_stream(kt->con, &(kt->send));
  }
  lprintf(1, "[debug] %s: close\n", __func__);
}

void mtnfs_accept_process(int l)
{
  ktask kt;
  pid_t pid;

  memset(&kt, 0, sizeof(kt));
  kt.addr.len = sizeof(kt.addr.addr);
  kt.con = accept(l, &(kt.addr.addr.addr), &(kt.addr.len));
  if(kt.con == -1){
    lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
    exit(0);
  }

  pid = fork();
  if(pid == -1){
    lprintf(0, "[error] %s: fork error %s\n", __func__, strerror(errno));
    close(kt.con);
    return;
  }
  if(pid){
    close(kt.con);
    return;
  }

  //----- child process -----
  close(l);
  mtnfs_child(&kt);
  close(kt.con);
  exit(0);
}

void do_loop(int m, int l)
{
  fd_set rfds;
  fd_set sfds;
  struct timeval tv;
  while(is_loop){
    waitpid(-1, NULL, WNOHANG);
    tv.tv_sec  = 1;
    tv.tv_usec = 0;
    FD_ZERO(&rfds);
    FD_ZERO(&sfds);
    FD_SET(l, &rfds);
    FD_SET(m, &rfds);
    if(kopt.task){
      FD_SET(m, &sfds);
    }
    if(select(1024, &rfds, &sfds, NULL, &tv) <= 0){
      continue;
    }
    if(FD_ISSET(m, &rfds)){
      mtnfs_udp_process(m);
    }
    if(FD_ISSET(m, &sfds)){
      mtnfs_task_process(m);
    }
    if(FD_ISSET(l, &rfds)){
      mtnfs_accept_process(l);
    }
  }
}

void do_daemon()
{
  int pid;

  if(!kopt.daemonize){
    return;
  }
  pid = fork();
  if(pid == -1){
    fprintf(stderr, "%s: can't fork()\n", __func__);
    exit(1); 
  }
  if(pid){
    _exit(0);
  }
  setsid();
  pid=fork();
  if(pid == -1){
    fprintf(stderr, "%s: can't fork()\n", __func__);
    exit(1); 
  }
  if(pid){
    _exit(0);
  }

  /*----- daemon process -----*/
  close(2);
  close(1);
  close(0);
  open("/dev/null",O_RDWR); /* new stdin  */
  dup(0);                   /* new stdout */
  dup(0);                   /* new stderr */
}

void signal_handler(int n)
{
  switch(n){
    case SIGINT:
    case SIGTERM:
      is_loop = 0;
      break;
    case SIGPIPE:
      break;
    case SIGUSR1:
      break;
    case SIGUSR2:
      break;
  }
}

void set_sig_handler()
{
  struct sigaction sig;
  memset(&sig, 0, sizeof(sig));
  sig.sa_handler = signal_handler;
  if(sigaction(SIGINT,  &sig, NULL) == -1){
    lprintf(0, "%s: sigaction error SIGINT\n", __func__);
    exit(1);
  }
  if(sigaction(SIGTERM, &sig, NULL) == -1){
    lprintf(0, "%s: sigaction error SIGTERM\n", __func__);
    exit(1);
  }
  if(sigaction(SIGPIPE, &sig, NULL) == -1){
    lprintf(0, "%s: sigaction error SIGPIPE\n", __func__);
    exit(1);
  }
  if(sigaction(SIGUSR1, &sig, NULL) == -1){
    lprintf(0, "%s: sigaction error SIGUSR1\n", __func__);
    exit(1);
  }
  if(sigaction(SIGUSR2, &sig, NULL) == -1){
    lprintf(0, "%s: sigaction error SIGUSR2\n", __func__);
    exit(1);
  }
}

struct option *get_optlist()
{
  static struct option opt[]={
      "pid",     1, NULL, 'P',
      "help",    0, NULL, 'h',
      "version", 0, NULL, 'v',
      "export",  1, NULL, 'e',
      0, 0, 0, 0
    };
  return(opt);
}

int main(int argc, char *argv[])
{
  int r;
  int daemonize = 1;
  char buff[PATH_MAX];
  if(argc < 2){
    usage();
    exit(0);
  }
  set_sig_handler();
  mtn_init_option();
  gethostname(kopt.host, sizeof(kopt.host));
  while((r = getopt_long(argc, argv, "hvne:l:f:m:p:P:D:", get_optlist(), NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'v':
        version();
        exit(0);

      case 'D':
        kopt.debuglevel = atoi(optarg);
        break;

      case 'n':
        kopt.daemonize = 0;
        break;

      case 'f':
        break;

      case 'e':
        if(chdir(optarg) == -1){
          lprintf(0,"error: %s %s\n", strerror(errno), optarg);
          exit(1);
        }
        break;

      case 'l':
        kopt.free_limit = atoikmg(optarg);
        break;

      case 'm':
        strcpy(kopt.mcast_addr, optarg);
        break;

      case 'p':
        kopt.mcast_port = atoi(optarg);
        break;

      case 'P':
        strcpy(kopt.pid, optarg);
        break;

      case '?':
        usage();
        exit(1);
    }
  }
  if((*kopt.pid != '/') && (*kopt.pid != 0)){
    sprintf(buff, "%s/%s", kopt.cwd, kopt.pid);
    strcpy(kopt.pid, buff);
  }
  do_daemon();
  mkpidfile();
  getcwd(kopt.cwd, PATH_MAX);
  start_message("======= mtnfs start =======\n");
  int m = create_msocket(kopt.mcast_port);
  int l = create_lsocket(kopt.mcast_port);
  if(listen(l, 5) == -1){
    lprintf(0, "%s: listen error\n", __func__);
    exit(1);
  }
  do_loop(m ,l);
  rmpidfile();
  lprintf(0, "mtnfs finished\n");
  return(0);
}

