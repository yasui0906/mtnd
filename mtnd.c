/*
 * mtnd.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"
#include "common.h"
#include "mtnd.h"

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

int is_freelimit()
{
  uint32_t bsize;
  uint32_t fsize;
  uint64_t dsize;
  uint64_t dfree;
  get_diskfree(&bsize, &fsize, &dsize, &dfree);
  dfree *= bsize;
  return(kopt.free_limit > dfree);
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

ktask *mtnd_task_create(kdata *data, kaddr *addr)
{
  lprintf(9, "[debug] %s: IN\n", __func__);
  ktask *kt;
  for(kt=kopt.task;kt;kt=kt->next){
    if(cmp_addr(&(kt->addr), addr) != 0){
      continue;
    }
    if(kt->recv.head.sqno != data->head.sqno){
      continue;
    }
    return(NULL);
  }
  kt = xmalloc(sizeof(ktask));
  memset(kt, 0, sizeof(ktask));
  gettimeofday(&(kt->tv),NULL);
  memcpy(&(kt->addr), addr, sizeof(kaddr));
  memcpy(&(kt->recv), data, sizeof(kdata));
  kt->type = data->head.type;
  lprintf(8, "[debug] %s: CMD=%s SEQ=%d\n", __func__, mtncmdstr[kt->type], data->head.sqno);
  if(kopt.task){
    kopt.task->prev = kt;
  }
  kt->next  = kopt.task;
  kopt.task = kt;
  lprintf(9, "[debug] %s: OUT\n", __func__);
  return(kt);
}

ktask *mtnd_task_cut(ktask *t)
{
  ktask *p = NULL;
  ktask *n = NULL;
  if(!t){
    return(NULL);
  }
  p = t->prev;
  n = t->next;
  if(p){
    p->next = n;
  }
  if(n){
    n->prev = p;
  }
  if(kopt.task == t){
    kopt.task = n;
  }
  if(kopt.tasksave == t){
    kopt.tasksave = n;
  }
  t->prev = NULL;
  t->next = NULL;
  return(n);
}

ktask *mtnd_task_delete(ktask *t)
{
  ktask *n;
  if(!t){
    return(NULL);
  }
  n = mtnd_task_cut(t);
  xfree(t);
  return(n);
}

ktask *mtnd_task_save(ktask *t)
{
  ktask *n;
  if(!t){
    return(NULL);
  }
  n = mtnd_task_cut(t);
  if(t->next = kopt.tasksave){
    t->next->prev = t;
  }
  kopt.tasksave = t;
  return(n);
}

//-------------------------------------------------------------------
// UDP PROSESS
//-------------------------------------------------------------------
static void mtnd_startup_process(ktask *kt)
{
  int len;
  kmember *mb;
  lprintf(9, "[debug] %s: IN mcount=%d\n", __func__, get_members_count(kopt.members));
  kt->addr.addr.in.sin_port = htons(kopt.mcast_port);
  kopt.members = add_member(kopt.members, &(kt->addr), NULL);
  mb = get_member(kopt.members, &(kt->addr));
  if(mb->host == NULL){
    len = mtn_get_string(NULL, &(kt->recv));
    mb->host = xmalloc(len);
    mtn_get_string(mb->host, &(kt->recv));
    lprintf(8, "[debug] %s: host=%s\n", __func__, mb->host);
  }
  gettimeofday(&(mb->tv),NULL);
  kt->fin = 1;
  kt->send.head.type = MTNCMD_STARTUP;
  kt->send.head.size = 0;
  kt->send.head.flag = 1;
  mtn_set_string(kopt.host, &(kt->send));
  lprintf(9, "[debug] %s: OUT mcount=%d\n", __func__, get_members_count(kopt.members));
}

static void mtnd_shutdown_process(ktask *kt)
{
  kmember *mb;
  lprintf(9, "[debug] %s: IN mcount=%d\n", __func__, get_members_count(kopt.members));
  kt->addr.addr.in.sin_port = htons(kopt.mcast_port);
  if(mb = get_member(kopt.members, &(kt->addr))){
    lprintf(8, "[debug] %s: host=%s\n", __func__, mb->host);
    if(mb == kopt.members){
      mb = (kopt.members = del_member(kopt.members));
    }else{
      mb = del_member(mb);
    }
  }
  kt->fin = 1;
  lprintf(9, "[debug] %s: OUT mcount=%d\n", __func__, get_members_count(kopt.members));
}

static void mtnd_hello_process(ktask *kt)
{
  ktask *t;
  uint32_t mcount;
  lprintf(9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  for(t=kopt.tasksave;t;t=t->next){
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
      mcount = get_members_count(kopt.members);
      mtn_set_string(kopt.host, &(kt->send));
      mtn_set_int(&mcount, &(kt->send), sizeof(mcount));
    }
  }
  lprintf(9, "[debug] %s: OUT FLAG=%d FIN=%d\n", __func__, kt->recv.head.flag, kt->fin);
}

static void mtnd_info_process(ktask *kt)
{
  meminfo mi;
  kmember mb;
  lprintf(9, "[debug] %s: IN\n", __func__);
  get_meminfo(&mi);
  mb.limit = kopt.free_limit;
  mb.malloccnt = malloccnt();
  mb.membercnt = get_members_count(kopt.members);
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
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

int mtnd_list_dir(ktask *kt)
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

static void mtnd_list_process(ktask *kt)
{
  lprintf(9, "[debug] %s: IN\n", __func__);
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
  lprintf(9, "[debug] %s: OUI\n", __func__);
}

static void mtnd_stat_process(ktask *kt)
{
  lprintf(9, "[debug] %s: IN\n", __func__);
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
      lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
      kt->send.head.type = MTNCMD_ERROR;
    }
  }else{
    kt->send.head.type = MTNCMD_SUCCESS;
    dirbase(kt->path, NULL, file);
    mtn_set_string(file, &(kt->send));
    mtn_set_stat(&(kt->stat), &(kt->send));
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_mkdir_process(ktask *kt)
{
  lprintf(9, "[debug] %s: IN\n", __func__);
  char buff[PATH_MAX];
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(buff, &(kt->recv));
  mtnd_fix_path(buff);
  sprintf(kt->path, "./%s", buff);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(mkdir_ex(kt->path) == -1){
    lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_rm_process(ktask *kt)
{
  lprintf(9, "[debug] %s: IN\n", __func__);
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
        lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        kt->send.head.type = MTNCMD_ERROR;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }else{
      if(unlink(kt->path) == -1){
        lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), kt->path);
        kt->send.head.type = MTNCMD_ERROR;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_rename_process(ktask *kt)
{
  lprintf(9, "[debug] %s: IN\n", __func__);
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
      lprintf(0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), obuff, nbuff);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_symlink_process(ktask *kt)
{
  lprintf(9, "[debug] %s: IN\n", __func__);
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
    lprintf(0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), oldpath, newpath);
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_readlink_process(ktask *kt)
{
  lprintf(9, "[debug] %s: IN\n", __func__);
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
    lprintf(0, "[error] %s: %s %s -> %s\n", __func__, strerror(errno), oldpath, newpath);
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }else{
    oldpath[size] = 0;
    mtn_set_string(oldpath, &(kt->send));
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_chmod_process(ktask *kt)
{
  mode_t mode;
  char path[PATH_MAX];
  lprintf(9, "[debug] %s: IN\n", __func__);
  kt->fin = 1;
  kt->send.head.fin  = 1;
  kt->send.head.size = 0;
  mtn_get_string(path, &(kt->recv));
  mtn_get_int(&mode, &(kt->recv), sizeof(mode));
  mtnd_fix_path(path);
  kt->send.head.type = MTNCMD_SUCCESS;
  if(chmod(path, mode) == -1){
    if(errno != ENOENT){
      lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_chown_process(ktask *kt)
{
  uid_t uid;
  gid_t gid;
  char path[PATH_MAX];
  lprintf(9, "[debug] %s: IN\n", __func__);
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
      lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

static void mtnd_utime_process(ktask *kt)
{
  struct utimbuf ut;
  char path[PATH_MAX];
  lprintf(9, "[debug] %s: IN\n", __func__);
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
      lprintf(0, "[error] %s: %s %s\n", __func__, strerror(errno), path);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

void mtnd_udp_process(int s)
{
  kaddr addr;
  kdata data;
  char a[64];
  lprintf(9, "[debug] %s: IN\n", __func__);
  addr.len = sizeof(addr.addr);
  if(recv_dgram(s, &data, &(addr.addr.addr), &(addr.len)) != -1){
    lprintf(8, "[debug] %s: from %s\n", __func__, v4addr(&addr, a, 64));
    mtnd_task_create(&data, &addr);
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

void mtnd_task_process(int s)
{
  lprintf(9, "[debug] %s: IN\n", __func__);
  ktask *kt = kopt.task;
  MTNFSTASKFUNC task;
  while(kt){
    kt->con = s;
    task = taskfunc[kt->type];
    if(task){
      task(kt);
    }else{
      kt->fin = 1;
      kt->send.head.fin  = 1;
      kt->send.head.size = 0;
      kt->send.head.type = MTNCMD_ERROR;
      lprintf(0, "[error] %s: Function Not Found type=%d\n", __func__, kt->type);
    }
    if(kt->fin){
      if(kt->recv.head.flag == 0){
        send_dgram(s, &(kt->send), &(kt->addr));
      }
      switch(kt->fin){
        case 1:
          kt = mtnd_task_delete(kt);
          break;
        case 2:
          kt = mtnd_task_save(kt);
          break;
      }
    }else{
      kt = kt->next;
    }
  }
  lprintf(9, "[debug] %s: OUT\n", __func__);
}

//------------------------------------------------------
// mtntool commands for TCP
//------------------------------------------------------
void mtnd_child_get(ktask *kt)
{
  lprintf(9, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  lprintf(8,"[info]  %s: PATH=%s\n", __func__, kt->path);
  kt->fd = open(kt->path, O_RDONLY);
  if(kt->fd == -1){
    lprintf(0,"[error] %s: file open error %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNCMD_ERROR;
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
        kt->send.head.type = MTNCMD_ERROR;
        kt->send.head.size = 0;
        kt->send.head.fin  = 1;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
        kt->fin = 1;
        break;
      }
      if(send_data_stream(kt->con, &(kt->send)) == -1){
        lprintf(0,"[error] %s: send error %s %s\n", __func__, strerror(errno), kt->path);
        kt->send.head.type = MTNCMD_ERROR;
        kt->send.head.size = 0;
        kt->send.head.fin  = 1;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
        kt->fin = 1;
        break;
      }
    }
  }
  lprintf(9, "[debug] %s: END\n", __func__);
}

void mtnd_child_set(ktask *kt)
{
  char d[PATH_MAX];
  char f[PATH_MAX];

  lprintf(9, "[debug] %s: START\n", __func__);
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
        kt->send.head.type = MTNCMD_ERROR;
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
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      kt->fd = creat(kt->path, kt->stat.st_mode);
      if(kt->fd == -1){
        lprintf(0,"[error] %s: can't creat %s %s\n", __func__, strerror(errno), kt->path);
        kt->fd  = 0;
        kt->fin = 1;
        kt->send.head.fin = 1;
        kt->send.head.type = MTNCMD_ERROR;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }
  lprintf(9, "[debug] %s: END\n", __func__);
}

//------------------------------------------------------
// mtnmount commands for TCP
//------------------------------------------------------
void mtnd_child_open(ktask *kt)
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
    lprintf(0, "[error] %s: %s, path=%s create=%d mode=%o\n", __func__, strerror(errno), kt->path, ((flags & O_CREAT) != 0), mode);
  }else{
    fstat(kt->fd, &(kt->stat));
    lprintf(1, "[info]  %s: path=%s create=%d mode=%o\n", __func__, kt->path, ((flags & O_CREAT) != 0), mode);
  }
}

void mtnd_child_read(ktask *kt)
{
  int r;
  size_t  size;
  off_t offset;
  mtn_get_int(&size,   &(kt->recv), sizeof(size));
  mtn_get_int(&offset, &(kt->recv), sizeof(offset));

  size = (size > MAX_DATASIZE) ? MAX_DATASIZE : size;
  r = pread(kt->fd, kt->send.data.data, size, offset);
  kt->send.head.fin  = 1;
  kt->send.head.size = (r > 0) ? r : 0;
}

void mtnd_child_write(ktask *kt)
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

void mtnd_child_close(ktask *kt)
{
  if(kt->fd){
    fstat(kt->fd, &(kt->stat));
    close(kt->fd);
    kt->fd = 0;
  }
}

void mtnd_child_truncate(ktask *kt)
{
  off_t offset;
  mtn_get_string(kt->path, &(kt->recv));
  mtn_get_int(&offset, &(kt->recv), sizeof(offset));
  mtnd_fix_path(kt->path);
  stat(kt->path, &(kt->stat));
  if(truncate(kt->path, offset) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    lprintf(0, "[error] %s: %s path=%s offset=%llu\n", __func__, strerror(errno), kt->path, offset);
  }else{
    lprintf(2, "[debug] %s: path=%s offset=%llu\n", __func__, kt->path, offset);
  }
}

void mtnd_child_mkdir(ktask *kt)
{
  lprintf(9, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path);
  if(mkdir(kt->path, 0777) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }
  lprintf(8, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  lprintf(9, "[debug] %s: END\n", __func__);
}

void mtnd_child_rmdir(ktask *kt)
{
  lprintf(9, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path);
  if(rmdir(kt->path) == -1){
    if(errno != ENOENT){
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
  lprintf(8, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  lprintf(9, "[debug] %s: END\n", __func__);
}

void mtnd_child_unlink(ktask *kt)
{
  lprintf(9, "[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  mtnd_fix_path(kt->path);
  if(lstat(kt->path, &(kt->stat)) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    lprintf(0,"[debug] %s: stat error: %s %s\n", __func__, strerror(errno), kt->path);
    return;
  }
  if(unlink(kt->path) == -1){
    kt->send.head.type = MTNCMD_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    lprintf(0,"[debug] %s: unlink error: %s %s\n", __func__, strerror(errno), kt->path);
    return;
  }
  lprintf(8, "[debug] %s: PATH=%s RES=%d\n", __func__, kt->path, kt->send.head.type);
  lprintf(9, "[debug] %s: END\n", __func__);
}

void mtnd_child_getattr(ktask *kt)
{
  lprintf(1, "[debug] %s: START\n", __func__);
  struct stat st;
  if(kt->fd){
    if(fstat(kt->fd, &st) == -1){
      lprintf(0, "[error] %s: 1\n", __func__);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      mtn_set_stat(&st, &(kt->send));
      lprintf(1, "[debug] %s: 2 size=%d\n", __func__, kt->send.head.size);
    }
  }else{
    mtn_get_string(kt->path, &(kt->recv));
    if(lstat(kt->path, &st) == -1){
      lprintf(0, "[error] %s: 3\n", __func__);
      kt->send.head.type = MTNCMD_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      lprintf(1, "[debug] %s: 4\n", __func__);
      mtn_set_stat(&st, &(kt->send));
    }
  }
  lprintf(1, "[debug] %s: END\n", __func__);
}

void mtnd_child_chmod(ktask *kt)
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

void mtnd_child_chown(ktask *kt)
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

void mtnd_child_setattr(ktask *kt)
{
  lprintf(0,"[debug] %s: START\n", __func__);
  mtn_get_string(kt->path, &(kt->recv));
  lprintf(0,"[debug] %s: END\n", __func__);
}

void mtnd_child_result(ktask *kt)
{
  memcpy(&(kt->send), &(kt->keep), sizeof(kdata));
  kt->keep.head.type = MTNCMD_SUCCESS;
  kt->keep.head.size = 0;
}

void mtnd_child_error(ktask *kt)
{
  errno = EACCES;
  lprintf(0, "[error] %s: TYPE=%u\n", __func__, kt->recv.head.type);
  kt->send.head.type = MTNCMD_ERROR;
  mtn_set_int(&errno, &(kt->send), sizeof(errno));
}

void mtnd_child(ktask *kt)
{
  char addr[64];
  MTNFSTASKFUNC call_func;
  MTNFSTASKFUNC func_list[MTNCMD_MAX];
  memset(func_list, 0, sizeof(func_list));
  func_list[MTNCMD_SET]      = mtnd_child_set;
  func_list[MTNCMD_GET]      = mtnd_child_get;
  func_list[MTNCMD_OPEN]     = mtnd_child_open;
  func_list[MTNCMD_READ]     = mtnd_child_read;
  func_list[MTNCMD_WRITE]    = mtnd_child_write;
  func_list[MTNCMD_CLOSE]    = mtnd_child_close;
  func_list[MTNCMD_TRUNCATE] = mtnd_child_truncate;
  func_list[MTNCMD_MKDIR]    = mtnd_child_mkdir;
  func_list[MTNCMD_RMDIR]    = mtnd_child_rmdir;
  func_list[MTNCMD_UNLINK]   = mtnd_child_unlink;
  func_list[MTNCMD_CHMOD]    = mtnd_child_chmod;
  func_list[MTNCMD_CHOWN]    = mtnd_child_chown;
  func_list[MTNCMD_GETATTR]  = mtnd_child_getattr;
  func_list[MTNCMD_SETATTR]  = mtnd_child_setattr;
  func_list[MTNCMD_RESULT]   = mtnd_child_result;

  v4addr(&(kt->addr), addr, sizeof(addr));
  lprintf(1, "[debug] %s: accept from %s:%d sock=%d\n", __func__, addr, v4port(&(kt->addr)), kt->con);
  kt->keep.head.ver  = PROTOCOL_VERSION;
  kt->keep.head.type = MTNCMD_SUCCESS;
  kt->keep.head.size = 0;
  while(is_loop && (kt->fin == 0)){
    kt->send.head.ver  = PROTOCOL_VERSION;
    kt->send.head.type = MTNCMD_SUCCESS;
    kt->send.head.size = 0;
    kt->res = recv_data_stream(kt->con, &(kt->recv));
    if(kt->res == -1){
      lprintf(0, "[error] %s: recv error type=%d sock=%d\n", __func__, kt->recv.head.type, kt->con);
      break;
    }
    if(kt->res == 1){
      break;
    }
    if(call_func = func_list[kt->recv.head.type]){
      call_func(kt);
    }else{
      mtnd_child_error(kt);
    }
    if(kt->recv.head.flag == 0){
      send_data_stream(kt->con, &(kt->send));
    }else{
      if(kt->send.head.type != MTNCMD_SUCCESS){
        memcpy(&(kt->keep), &(kt->send), sizeof(kdata));
      }
    }
  }
  lprintf(1, "[debug] %s: close\n", __func__);
}

void mtnd_accept_process(int l)
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
  mtnd_child(&kt);
  close(kt.con);
  exit(0);
}

void mtnd_loop(int e, int l)
{
  int      r;
  ktask   *t;
  kmember *m;
  struct timeval tv;
  struct timeval tv_health;
  struct epoll_event ev[2];
  gettimeofday(&tv_health, NULL);

  /*===== Main Loop =====*/
  while(is_loop){
    waitpid(-1, NULL, WNOHANG);
    r = epoll_wait(e, ev, 2, 1000);
    gettimeofday(&tv, NULL);
    if((tv.tv_sec - tv_health.tv_sec) > 60){
      mtn_startup(1);
      memcpy(&tv_health, &tv, sizeof(tv));
    }
    m = kopt.members;
    while(m){
      if((tv.tv_sec - m->tv.tv_sec) > 300){
        if(m == kopt.members){
          m = del_member(m);
          kopt.members = m;
        }else{
          m = del_member(m);
        }
        continue;
      }
      m = m->next;
    }
    if(r == 0){
      t = kopt.tasksave;
      while(t){
        if((tv.tv_sec - t->tv.tv_sec) > 30){
          t = mtnd_task_delete(t);
        }else{
          t = t->next;
        }
      }
      continue;
    }
    if(r == -1){
      if(errno != EINTR){
        lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
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
        ev[r].events = kopt.task ? EPOLLIN | EPOLLOUT : EPOLLIN;
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
    lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }

  m = create_msocket(kopt.mcast_port);
  if(m == -1){
    lprintf(0, "[error] %s: can't socket create\n", __func__);
    return;
  }

  l = create_lsocket(kopt.mcast_port);
  if(l == -1){
    lprintf(0, "[error] %s: can't socket create\n", __func__);
    return;
  }

  if(listen(l, 64) == -1){
    lprintf(0, "%s: listen error\n", __func__);
    return;
  }

  ev.data.fd = l;
  ev.events  = EPOLLIN;
  if(epoll_ctl(e, EPOLL_CTL_ADD, l, &ev) == -1){
    lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }

  ev.data.fd = m;
  ev.events  = EPOLLIN;
  if(epoll_ctl(e, EPOLL_CTL_ADD, m, &ev) == -1){
    lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }
  mtn_startup(0);
  mtnd_loop(e,l);
  mtn_shutdown();
  close(e);
  close(m);
  close(l);
}

void daemonize()
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
  pid = fork();
  if(pid == -1){
    fprintf(stderr, "%s: can't fork()\n", __func__);
    exit(1); 
  }
  if(pid){
    _exit(0);
  }

  /*----- daemon process -----*/
  kopt.use_syslog = 1;
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
      kopt.debuglevel++;
      break;
    case SIGUSR2:
      kopt.debuglevel--;
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

void init_task()
{
  memset(taskfunc, 0, sizeof(taskfunc));
  taskfunc[MTNCMD_STARTUP]  = (MTNFSTASKFUNC)mtnd_startup_process;
  taskfunc[MTNCMD_SHUTDOWN] = (MTNFSTASKFUNC)mtnd_shutdown_process;
  taskfunc[MTNCMD_HELLO]    = (MTNFSTASKFUNC)mtnd_hello_process;
  taskfunc[MTNCMD_INFO]     = (MTNFSTASKFUNC)mtnd_info_process;
  taskfunc[MTNCMD_LIST]     = (MTNFSTASKFUNC)mtnd_list_process;
  taskfunc[MTNCMD_STAT]     = (MTNFSTASKFUNC)mtnd_stat_process;
  taskfunc[MTNCMD_MKDIR]    = (MTNFSTASKFUNC)mtnd_mkdir_process;
  taskfunc[MTNCMD_RMDIR]    = (MTNFSTASKFUNC)mtnd_rm_process;
  taskfunc[MTNCMD_UNLINK]   = (MTNFSTASKFUNC)mtnd_rm_process;
  taskfunc[MTNCMD_RENAME]   = (MTNFSTASKFUNC)mtnd_rename_process;
  taskfunc[MTNCMD_SYMLINK]  = (MTNFSTASKFUNC)mtnd_symlink_process;
  taskfunc[MTNCMD_READLINK] = (MTNFSTASKFUNC)mtnd_readlink_process;
  taskfunc[MTNCMD_CHMOD]    = (MTNFSTASKFUNC)mtnd_chmod_process;
  taskfunc[MTNCMD_CHOWN]    = (MTNFSTASKFUNC)mtnd_chown_process;
  taskfunc[MTNCMD_UTIME]    = (MTNFSTASKFUNC)mtnd_utime_process;
}

void parse(int argc, char *argv[])
{
  int r;
  if(argc < 2){
    usage();
    exit(0);
  }
  while((r = getopt_long(argc, argv, "hvne:l:m:p:P:D:", get_optlist(), NULL)) != -1){
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
        if(*optarg == '/'){
          strcpy(kopt.pid, optarg);
        }else{
          sprintf(kopt.pid, "%s/%s", kopt.cwd, optarg);
        }
        break;

      case '?':
        usage();
        exit(1);
    }
  }
  getcwd(kopt.cwd, sizeof(kopt.cwd));
}

int mtnd()
{
  uint32_t bsize;
  uint32_t fsize;
  uint64_t dsize;
  uint64_t dfree;
  get_diskfree(&bsize, &fsize, &dsize, &dfree);
  lprintf(0, "======= %s start =======\n", MODULE_NAME);
  lprintf(0, "ver  : %s\n", MTN_VERSION);
  lprintf(0, "pid  : %d\n", getpid());
  lprintf(0, "debug: %d\n", kopt.debuglevel);
  lprintf(0, "mcast: %s\n", kopt.mcast_addr);
  lprintf(0, "port : %d\n", kopt.mcast_port);
  lprintf(0, "host : %s\n", kopt.host);
  lprintf(0, "base : %s\n", kopt.cwd);
  lprintf(0, "size : %6llu [MB]\n", fsize * dsize / 1024 / 1024);
  lprintf(0, "free : %6llu [MB]\n", bsize * dfree / 1024 / 1024);
  lprintf(0, "limit: %6llu [MB]\n", kopt.free_limit/1024 / 1024);
  mkpidfile();
  mtnd_main();
  rmpidfile();
  lprintf(0, "%s finished\n", MODULE_NAME);
  return(0);
}

int main(int argc, char *argv[])
{
  mtn_init(MODULE_NAME);
  set_sig_handler();
  parse(argc, argv);
  daemonize();
  init_task();
  return(mtnd());
}

