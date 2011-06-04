/*
 * mtnfs.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"
typedef void (*MTNFSTASKFUNC)(ktask *);
MTNFSTASKFUNC task_func[MTNCMD_MAX];

void version()
{
  lprintf(0, "%s version %s\n", MODULE_NAME, PACKAGE_VERSION);
}

void usage()
{
  version();
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

uint32_t get_freesize()
{
  uint64_t size = 0;
  struct statvfs vf;
  if(kopt.freesize){
    return(kopt.freesize);
  }
  if(statvfs(".", &vf) == -1){
    lprintf(0, "error: %s\n", strerror(errno));
    return(0);
  }
  size = vf.f_bfree * vf.f_bsize;
  size /= 1024;
  size /= 1024;
  return(size);
}

uint32_t get_datasize(char *path)
{
  char full[PATH_MAX];
  off_t size = 0;
  DIR *d = opendir(path);
  struct dirent *ent;
  struct stat st;

  lprintf(0,"%s: %s\n", __func__, path);
  while(ent = readdir(d)){
    if(ent->d_name[0] == '.'){
      continue;
    }
    sprintf(full, "%s/%s", path, ent->d_name);
    if(lstat(full, &st) == -1){
      lprintf(0, "%s: %s %s\n", __func__, strerror(errno), full);
      continue;
    }
    if(S_ISDIR(st.st_mode)){
      size += get_datasize(full);
      continue;
    }
    if(S_ISREG(st.st_mode)){
      size += st.st_size;
      lprintf(0,"%s: file=%s size=%llu\n", __func__, full, st.st_size);
    }
  }
  closedir(d);
  lprintf(0,"%s: size1=%u\n",__func__, size);
  size /= 1024;
  size /= 1024;
  lprintf(0,"%s: size2=%u\n",__func__, size);
  return(size);
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
  if(!(task->fin)){
    return(task->next);
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

static void mtnfs_hello_process(ktask *kt)
{
  lprintf(0,"%s: START\n", __func__);
  mtn_set_string(kopt.host, &(kt->send));
  send_dgram(kt->con, &(kt->send), &(kt->addr));
  kt->fin = 1;
}

static void mtnfs_info_process(ktask *kt)
{
  lprintf(1,"%s: START\n", __func__);
  uint32_t size = get_freesize();
  kt->send.head.fin = 1;
  mtn_set_int(&size, &(kt->send), sizeof(size));
  send_dgram(kt->con, &(kt->send), &(kt->addr));
  kt->fin = 1;
}

static void mtnfs_list_process(ktask *kt)
{
  lprintf(0,"%s: START\n", __func__);
  uint16_t len;
  struct dirent *ent;
  char buff[PATH_MAX];
  char full[PATH_MAX];

  if(kt->dir){
    while(ent = readdir(kt->dir)){
      if(ent->d_name[0] == '.'){
        continue;
      }
      sprintf(full, "%s/%s", kt->path, ent->d_name);
      if(lstat(full, &(kt->stat)) == 0){
        len  = mtn_set_string(ent->d_name, NULL);
        len += mtn_set_stat(&(kt->stat),   NULL);
        if(kt->send.head.size + len > kopt.max_packet_size){
          send_dgram(kt->con, &(kt->send), &(kt->addr));
          memset(&(kt->send), 0, sizeof(kt->send));
          mtn_set_string(ent->d_name, &(kt->send));
          mtn_set_stat(&(kt->stat),   &(kt->send));
          return;
        }
        mtn_set_string(ent->d_name, &(kt->send));
        mtn_set_stat(&(kt->stat),   &(kt->send));
      } 
    }
    closedir(kt->dir);
    kt->fin = 1;
    kt->dir = NULL;
    kt->send.head.fin = 1;
    send_dgram(kt->con, &(kt->send), &(kt->addr));
    return;
  }

  kt->send.head.size = 0;
  mtn_get_string(buff, &(kt->recv));
  mtnfs_fix_path(buff);
  sprintf(kt->path, "./%s", buff);

  if(lstat(kt->path, &(kt->stat)) == -1){
    lprintf(0, "%s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->fin = 1;
    kt->send.head.fin = 1;
    send_dgram(kt->con, &(kt->send), &(kt->addr));
    return;
  }
  if(S_ISREG(kt->stat.st_mode)){
    mtn_set_string(basename(kt->path), &(kt->send));
    mtn_set_stat(&(kt->stat), &(kt->send));
    kt->fin = 1;
    kt->send.head.fin = 1;
    send_dgram(kt->con, &(kt->send), &(kt->addr));
    return;
  }
  if(S_ISDIR(kt->stat.st_mode)){
    kt->dir = opendir(kt->path);
    return;
  }
  kt->fin = 1;
}

void mtnfs_udp_process(int s)
{
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
  printf("%s: type=%d size=%d ver=%d from=%s:%d\n", __func__, type, size, ver, inet_ntoa(addr.addr.in.sin_addr), ntohs(addr.addr.in.sin_port));
  // for debug end
}

void mtnfs_task_process(int s)
{
  ktask *kt = kopt.task;
  while(kt){
    kt->con = s;
    MTNFSTASKFUNC task = task_func[kt->type];
    if(task){
      task(kt);
    }else{
      lprintf(0, "%s: Function Not Found %d\n", __func__, kt->type);
    }
    kt = mtnfs_task_delete(kt);
  }
}

//------------------------------------------------------
// mtntool commands
//------------------------------------------------------
void mtnfs_child_get(ktask *kt)
{
  mtn_get_string(kt->path, &(kt->recv));
  lprintf(0,"%s: %s\n", __func__, kt->path);
  kt->fd = open(kt->path, O_RDONLY);
  if(kt->fd == -1){
    lprintf(0,"%s: %s %s\n", __func__, strerror(errno), kt->path);
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
  }else{
    while(is_loop){
      kt->send.head.size = read(kt->fd, kt->send.data.data, sizeof(kt->send.data.data));
      if(kt->send.head.size == -1){
        lprintf(0,"%s: %s %s\n", __func__, strerror(errno), kt->path);
        kt->send.head.type = MTNRES_ERROR;
        kt->send.head.size = 0;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
        break;
      }
      if(kt->send.head.size == 0){
        close(kt->fd);
        kt->fd = 0;
        break;
      }
      send_data_stream(kt->con, &(kt->send));
    }
  }
}

void mtnfs_child_set(ktask *kt)
{
  int r;
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
      kt->fd = 0;
    }else{
      //----- write -----
      r = write(kt->fd, kt->recv.data.data, kt->recv.head.size);
      if(r == -1){
        printf("%s: %s\n",__func__, strerror(errno));
        kt->send.head.type = MTNRES_ERROR;
        mtn_set_int(&errno, &(kt->send), sizeof(errno));
      }
    }
  }else{
    //----- open/create -----
    mtn_get_string(kt->path,  &(kt->recv));
    mtn_get_stat(&(kt->stat), &(kt->recv));
    lprintf(0,"%s: %s\n", __func__, kt->path);
    kt->fd = creat(kt->path, kt->stat.st_mode);
    if(kt->fd == -1){
      kt->fd = 0;
      kt->send.head.type = MTNRES_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }
  }
}

//------------------------------------------------------
// mtnmount commands
//------------------------------------------------------
void mtnfs_child_open(ktask *kt)
{
  int   flags;
  mode_t mode;
  mtn_get_string(kt->path, &(kt->recv));
  mtn_get_int(&flags, &(kt->recv), sizeof(flags));
  mtn_get_int(&mode,  &(kt->recv), sizeof(mode));
  mtnfs_fix_path(kt->path);
  kt->fd = open(kt->path, flags, mode);
  if(kt->fd == -1){
    kt->fd = 0;
    kt->send.head.type = MTNRES_ERROR;
    mtn_set_int(&errno, &(kt->send), sizeof(errno));
    lprintf(0, "%s: [info] NG path=%s create=%d mode=%o\n", __func__, kt->path, ((flags & O_CREAT) != 0), mode);
  }else{
    lprintf(0, "%s: [info] PK path=%s create=%d mode=%o\n", __func__, kt->path, ((flags & O_CREAT) != 0), mode);
  }
}

void mtnfs_child_read(ktask *kt)
{
  int r;
  size_t  size;
  off_t offset;
  mtn_get_int(&size,   &(kt->recv), sizeof(size));
  mtn_get_int(&offset, &(kt->recv), sizeof(offset));
  lprintf(0, "%s: path=%s size=%d off=%llu\n", __func__, kt->path, size, offset);

  lseek(kt->fd, offset, SEEK_SET);
  while(size){
    if(size < MAX_DATASIZE){
      r = read(kt->fd, kt->send.data.data, size);
    }else{
      r = read(kt->fd, kt->send.data.data, MAX_DATASIZE);
    }
    lprintf(0, "%s: read=%d\n", __func__, r);
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
  int r;
  size_t  size;
  void   *buff;
  off_t offset;

  mtn_get_int(&offset, &(kt->recv), sizeof(offset));
  size = kt->recv.head.size;
  buff = kt->recv.data.data;
  lprintf(0, "%s: 1 path=%s size=%d off=%llu\n", __func__, kt->path, size, offset);
  lseek(kt->fd, offset, SEEK_SET);
  while(size){
    r = write(kt->fd, buff, size);
    if(r == -1){
      kt->send.head.type = MTNRES_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
      break;
    }
    buff += r;
    size -= r;
  }
  lprintf(0, "%s: 2 res=%d\n", __func__, (int)(kt->send.head.type));
}

void mtnfs_child_close(ktask *kt)
{
  if(kt->fd){
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
}

void mtnfs_child_getattr(ktask *kt)
{
  lprintf(0, "%s: 0\n", __func__);
  struct stat st;
  if(kt->fd){
    if(fstat(kt->fd, &st) == -1){
      lprintf(0, "%s: 1\n", __func__);
      kt->send.head.type = MTNRES_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      lprintf(0, "%s: 2\n", __func__);
      mtn_set_stat(&st, &(kt->send));
    }
  }else{
    mtn_get_string(kt->path, &(kt->recv));
    if(lstat(kt->path, &st) == -1){
      lprintf(0, "%s: 3\n", __func__);
      kt->send.head.type = MTNRES_ERROR;
      mtn_set_int(&errno, &(kt->send), sizeof(errno));
    }else{
      lprintf(0, "%s: 4\n", __func__);
      mtn_set_stat(&st, &(kt->send));
    }
  }
}

void mtnfs_child_setattr(ktask *kt)
{
}

void mtnfs_child_error(ktask *kt)
{
  errno = EACCES;
  lprintf(0, "%s: [error] TYPE=%u\n", __func__, kt->recv.head.type);
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
  func_list[MTNCMD_GETATTR]  = mtnfs_child_getattr;
  func_list[MTNCMD_SETATTR]  = mtnfs_child_setattr;
  lprintf(0,"%s: PID=%d accept from %s:%d\n", __func__, getpid(), inet_ntoa(kt->addr.addr.in.sin_addr), ntohs(kt->addr.addr.in.sin_port));
  while(is_loop && (kt->fin == 0)){
    kt->send.head.ver  = PROTOCOL_VERSION;
    kt->send.head.type = MTNRES_SUCCESS;
    kt->send.head.size = 0;
    kt->res = recv_data_stream(kt->con, &(kt->recv));
    if(kt->res == -1){
      lprintf(0, "%s: [error] recv error\n", __func__);
      break;
    }
    if(kt->res == 1){
      lprintf(0, "%s: [info] remote close\n", __func__);
      break;
    }
    lprintf(0,"%s: [info] pid=%d recv_type=%d recv_size=%d\n", __func__, getpid(), kt->recv.head.type, kt->recv.head.size);
    if(call_func = func_list[kt->recv.head.type]){
      call_func(kt);
      send_stream(kt->con, &(kt->send));
    }else{
      mtnfs_child_error(kt);
    }
  }
  lprintf(0, "%s: PID=%d END\n", __func__, getpid());
}

void mtnfs_accept_process(int l)
{
  ktask kt;
  pid_t pid;
  memset(&kt, 0, sizeof(kt));
  kt.addr.len = sizeof(kt.addr.addr);
  kt.con = accept(l, &(kt.addr.addr.addr), &(kt.addr.len));
  if(kt.con == -1){
    lprintf(0, "%s: [error] %s\n", __func__, strerror(errno));
    exit(0);
  }

  pid = fork();
  if(pid == -1){
    lprintf(0, "%s: [error] fork error %s\n", __func__, strerror(errno));
    close(kt.con);
    return;
  }
  if(pid){
    close(kt.con);
    return;
  }

  //----- child process -----
  lprintf(0, "%s: [info] PID=%d CHILD START\n", __func__, getpid());
  close(l);
  mtnfs_child(&kt);
  close(kt.con);
  lprintf(0, "%s: [info] PID=%d CHILD END\n", __func__, getpid());
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

int main(int argc, char *argv[])
{
  int r;
  int daemonize = 1; 
  struct option opt[4];
  opt[0].name    = "help";
  opt[0].has_arg = 0;
  opt[0].flag    = NULL;
  opt[0].val     = 'h';

  opt[1].name    = "version";
  opt[1].has_arg = 0;
  opt[1].flag    = NULL;
  opt[1].val     = 'v';

  opt[2].name    = "export";
  opt[2].has_arg = 1;
  opt[2].flag    = NULL;
  opt[2].val     = 'e';

  opt[3].name    = NULL;
  opt[3].has_arg = 0;
  opt[3].flag    = NULL;
  opt[3].val     = 0;

  set_sig_handler();
  mtn_init_option();
  gethostname(kopt.host, sizeof(kopt.host));

  memset(task_func, 0, sizeof(task_func));
  task_func[MTNCMD_HELLO] = (MTNFSTASKFUNC)mtnfs_hello_process;
  task_func[MTNCMD_INFO]  = (MTNFSTASKFUNC)mtnfs_info_process;
  task_func[MTNCMD_LIST]  = (MTNFSTASKFUNC)mtnfs_list_process;

  while((r = getopt_long(argc, argv, "hvne:l:f:", opt, NULL)) != -1){
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
        kopt.freesize = atoi(optarg);
        break;

      case 'e':
        if(chdir(optarg) == -1){
          lprintf(0,"error: %s %s\n", strerror(errno), optarg);
          exit(1);
        }
        break;

      case 'l':
        kopt.limitsize = atoi(optarg);
        break;

      case '?':
        usage();
        exit(1);
    }
  }

  kopt.cwd = getcwd(NULL, 0);
  kopt.freesize = get_freesize();
  kopt.datasize = get_datasize(kopt.cwd);

  lprintf(0, "======= mtnfs start =======\n");
  version();
  lprintf(0, "host: %s\n", kopt.host);
  lprintf(0, "base: %s\n", kopt.cwd);
  lprintf(0, "free: %u[MB]\n", kopt.freesize);
  lprintf(0, "used: %u[MB]\n", kopt.datasize);

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

