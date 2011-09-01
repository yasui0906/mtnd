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
  printf("   -h      # help\n");
  printf("   -v      # version\n");
  printf("   -n      # no daemon\n");
  printf("   -e dir  # export dir\n");
  printf("   -l size # limit size (MB)\n");
  printf("\n");
}

int mkdir_ex(const char *path)
{
  struct stat st;
  char d[PATH_MAX];
  char f[PATH_MAX];
  dirbase(path,d,f);
  if(stat(path, &st) == 0){
    return(0);
  }
  if(stat(d, &st) == -1){
    if(mkdir_ex(d) == -1){
      return(-1);
    }
  }
  return(mkdir(path, 0755));
}

uint64_t get_diskfree()
{
  uint64_t size = 0;
  struct statvfs vf;
  if(statvfs(".", &vf) == -1){
    lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
    return(0);
  }
  size = vf.f_bfree * vf.f_bsize;
  return(size);
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
  lprintf(0, "%s", msg);
  version();
  lprintf(0, "host: %s\n", kopt.host);
  lprintf(0, "base: %s\n", kopt.cwd);
  lprintf(0, "free: %llu [KB]\n", get_diskfree() / 1024);
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
  lprintf(0,"[debug] %s: START\n", __func__);
  mtn_set_string(kopt.host, &(kt->send));
  kt->fin = 1;
  lprintf(0,"[debug] %s: END\n", __func__);
}

static void mtnfs_info_process(ktask *kt)
{
  lprintf(1,"[debug] %s: START\n", __func__);
  uint64_t size = get_diskfree();
  kt->send.head.fin = 1;
  mtn_set_int(&size, &(kt->send), sizeof(size));
  kt->fin = 1;
  lprintf(1,"[debug] %s: END\n", __func__);
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
  lprintf(0,"[debug] %s: START\n", __func__);
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
      lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
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
  lprintf(0,"[debug] %s: END\n", __func__);
}

static void mtnfs_stat_process(ktask *kt)
{
  lprintf(0,"[debug] %s: START\n", __func__);
  char buff[PATH_MAX];
  char file[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(buff, &(kt->recv));
  mtnfs_fix_path(buff);
  sprintf(kt->path, "./%s", buff);
  if(lstat(kt->path, &(kt->stat)) == -1){
    lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNRES_ERROR;
  }else{
    kt->send.head.type = MTNRES_SUCCESS;
    dirbase(kt->path, NULL, file);
    mtn_set_string(file, &(kt->send));
    mtn_set_stat(&(kt->stat), &(kt->send));
  }
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnfs_udp_process(int s)
{
  lprintf(0,"[debug] %s: START\n", __func__);
  kaddr addr;
  kdata data;
  addr.len = sizeof(addr.addr);
  if(recv_dgram(s, &data, &(addr.addr.addr), &(addr.len)) == -1){
    return;
  }
  mtnfs_task_create(&data, &addr);
  // for debug top
  int ver  = data.head.ver;
  int type = data.head.type;
  int size = data.head.size;
  lprintf(0,"[debug] %s: type=%d size=%d ver=%d from=%s:%d\n", __func__, type, size, ver, inet_ntoa(addr.addr.in.sin_addr), ntohs(addr.addr.in.sin_port));
  // for debug end
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnfs_task_process(int s)
{
  lprintf(0,"[debug] %s: START\n", __func__);
  ktask *kt = kopt.task;
  MTNFSTASKFUNC taskfunc[MTNCMD_MAX];
  memset(taskfunc, 0, sizeof(taskfunc));
  taskfunc[MTNCMD_HELLO] = (MTNFSTASKFUNC)mtnfs_hello_process;
  taskfunc[MTNCMD_INFO]  = (MTNFSTASKFUNC)mtnfs_info_process;
  taskfunc[MTNCMD_LIST]  = (MTNFSTASKFUNC)mtnfs_list_process;
  taskfunc[MTNCMD_STAT]  = (MTNFSTASKFUNC)mtnfs_stat_process;
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
  lprintf(0,"[debug] %s: END\n", __func__);
}

//------------------------------------------------------
// mtntool commands for TCP
//------------------------------------------------------
void mtnfs_child_get(ktask *kt)
{
  lprintf(0,"[debug] %s: START\n", __func__);
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
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnfs_child_set(ktask *kt)
{
  char d[PATH_MAX];
  char f[PATH_MAX];

  lprintf(0,"[debug] %s: START\n", __func__);
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
  lprintf(0,"[debug] %s: END\n", __func__);
}

//------------------------------------------------------
// mtnmount commands for TCP
//------------------------------------------------------
void mtnfs_child_open(ktask *kt)
{
  char  d[PATH_MAX];
  char  f[PATH_MAX];
  int   flags;
  mode_t mode;
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
    lprintf(0, "[info]  %s: NG path=%s create=%d mode=%o\n", __func__, kt->path, ((flags & O_CREAT) != 0), mode);
  }else{
    fstat(kt->fd, &(kt->stat));
    lprintf(0, "[info]  %s: OK path=%s create=%d mode=%o\n", __func__, kt->path, ((flags & O_CREAT) != 0), mode);
  }
}

void mtnfs_child_read(ktask *kt)
{
  int r;
  size_t  size;
  off_t offset;
  lprintf(0,"[debug] %s: START\n", __func__);
  mtn_get_int(&size,   &(kt->recv), sizeof(size));
  mtn_get_int(&offset, &(kt->recv), sizeof(offset));
  lprintf(0,"[debug] %s: path=%s size=%d off=%llu\n", __func__, kt->path, size, offset);

  lseek(kt->fd, offset, SEEK_SET);
  while(size){
    if(size < MAX_DATASIZE){
      r = read(kt->fd, kt->send.data.data, size);
    }else{
      r = read(kt->fd, kt->send.data.data, MAX_DATASIZE);
    }
    lprintf(0,"[debug] %s: read=%d\n", __func__, r);
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
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnfs_child_write(ktask *kt)
{
  size_t  size;
  void   *buff;
  off_t offset;

  lprintf(0,"[debug] %s: START\n", __func__);
  mtn_get_int(&offset, &(kt->recv), sizeof(offset));
  size = kt->recv.head.size;
  buff = kt->recv.data.data;
  lprintf(0,"[debug] %s: path=%s size=%d off=%llu\n", __func__, kt->path, size, offset);
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
  lprintf(0,"[debug] %s: res=%d\n", __func__, (int)(kt->send.head.type));
  lprintf(0,"[debug] %s: END\n", __func__);
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
  lprintf(0,"[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnfs_fix_path(kt->path);
  if(mkdir(kt->path, 0777) == -1){
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(0,"[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnfs_child_rmdir(ktask *kt)
{
  lprintf(0,"[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnfs_fix_path(kt->path);
  if(rmdir(kt->path) == -1){
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(0,"[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnfs_child_unlink(ktask *kt)
{
  lprintf(0,"[debug] %s: START\n", __func__);
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
  lprintf(0,"[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnfs_child_getattr(ktask *kt)
{
  lprintf(0, "[debug] %s: START\n", __func__);
  struct stat st;
  if(kt->fd){
    if(fstat(kt->fd, &st) == -1){
      lprintf(0, "[error] %s: 1\n", __func__);
      kt->send.head.type = MTNRES_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      lprintf(0, "[debug] %s: 2\n", __func__);
      mtn_set_stat(&st, &(kt->send));
    }
  }else{
    mtn_get_string(kt->path, &(kt->recv));
    if(lstat(kt->path, &st) == -1){
      lprintf(0, "[error] %s: 3\n", __func__);
      kt->send.head.type = MTNRES_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      lprintf(0, "[debug] %s: 4\n", __func__);
      mtn_set_stat(&st, &(kt->send));
    }
  }
  lprintf(0, "[debug] %s: END\n", __func__);
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
  lprintf(0,"[debug] %s: PID=%d accept from %s:%d\n", __func__, getpid(), inet_ntoa(kt->addr.addr.in.sin_addr), ntohs(kt->addr.addr.in.sin_port));
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
      lprintf(0, "[info]  %s: remote close\n", __func__);
      break;
    }
    lprintf(0,"[info]  %s: pid=%d recv_type=%d recv_size=%d\n", __func__, getpid(), kt->recv.head.type, kt->recv.head.size);
    if(call_func = func_list[kt->recv.head.type]){
      call_func(kt);
    }else{
      mtnfs_child_error(kt);
    }
    send_stream(kt->con, &(kt->send));
  }
  lprintf(0, "[debug] %s: PID=%d END\n", __func__, getpid());
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
  lprintf(0, "[info]  %s: PID=%d CHILD START\n", __func__, getpid());
  close(l);
  mtnfs_child(&kt);
  close(kt.con);
  lprintf(0, "[info]  %s: PID=%d CHILD END\n", __func__, getpid());
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

void do_daemon(int m, int l)
{
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
  if(argc < 2){
    usage();
    exit(0);
  }
  set_sig_handler();
  mtn_init_option();
  gethostname(kopt.host, sizeof(kopt.host));
  while((r = getopt_long(argc, argv, "hvne:l:f:", get_optlist(), NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'v':
        version();
        exit(0);

      case 'n':
        daemonize = 0;
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
        break;

      case '?':
        usage();
        exit(1);
    }
  }
  kopt.cwd = getcwd(NULL, 0);
  start_message("======= mtnfs start =======\n");
  int m = create_msocket(6000);
  int l = create_lsocket(6000);
  if(listen(l, 5) == -1){
    lprintf(0, "%s: listen error\n", __func__);
    exit(1);
  }
  if(daemonize){
    do_daemon(m, l);
  }else{
    do_loop(m ,l);
  }
  lprintf(0, "mtnfs finished\n");
  return(0);
}

