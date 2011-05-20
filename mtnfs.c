/*
 * mtnfs.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"

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
      lprintf(0,"%s: file=%s size=%d\n", __func__, full, (int)st.st_size);
    }
  }
  closedir(d);
  size /= 1024;
  size /= 1024;
  return(size);
}


void mtnfs_list_process(kdata *sdata, kdata *rdata)
{
  //lprintf(0,"%s: START\n", __func__);
  struct stat st;
  struct dirent *ent;
  char path[PATH_MAX];
  char full[PATH_MAX];
  uint8_t *buff;

  sdata->head.size = 0;
  if(rdata->head.size){
    sprintf(path, "./%s", rdata->data.data);
  }else{
    strcpy(path, "./");
  }
  if(stat(path, &st) == -1){
    lprintf(0, "%s: %s %s\n", __func__, strerror(errno), path);
    return;
  }
  if(S_ISREG(st.st_mode)){
    mtn_set_string(basename(path), sdata);
    buff = sdata->data.data + sdata->head.size;
  }
  if(S_ISDIR(st.st_mode)){
    DIR *d = opendir(path);
    while(ent = readdir(d)){
      if(ent->d_name[0] == '.'){
        continue;
      }
      sprintf(full, "%s/%s", path, ent->d_name);
      if(lstat(full, &st) == 0){
        mtn_set_string(ent->d_name, sdata);
        mtn_set_int(&(st.st_mode),  sdata, sizeof(st.st_mode));
        mtn_set_int(&(st.st_size),  sdata, sizeof(st.st_size));
        mtn_set_int(&(st.st_uid),   sdata, sizeof(st.st_uid));
        mtn_set_int(&(st.st_gid),   sdata, sizeof(st.st_gid));
        mtn_set_int(&(st.st_atime), sdata, sizeof(st.st_atime));
        mtn_set_int(&(st.st_mtime), sdata, sizeof(st.st_mtime));
      } 
    }
    closedir(d);
  }
}

void mtnfs_hello_process(kdata *sdata, kdata *rdata)
{
  //lprintf(0,"%s: START\n", __func__);
  sdata->head.size = strlen(kopt.host) + 1;
  memcpy(sdata->data.data, kopt.host, sdata->head.size);
}

void mtnfs_info_process(kdata *sdata, kdata *rdata)
{
  //lprintf(1,"%s: START\n", __func__);
  kinfo *info = &(sdata->data.info);
  sdata->head.size = sizeof(kinfo);
  info->free = get_freesize();
  info->host = (uint8_t *)NULL + sdata->head.size;
  strcpy(sdata->data.data + sdata->head.size, kopt.host);
  sdata->head.size += strlen(kopt.host) + 1;
}

void mtnfs_udp_process(int s)
{
  kaddr  addr;
  kdata sdata;
  kdata rdata;
  uint8_t *sbuff = sdata.data.data;
  uint8_t *rbuff = rdata.data.data;

  addr.len = sizeof(addr.addr);
  if(recv_dgram(s, &rdata, &(addr.addr.addr), &(addr.len)) == -1){
    return;
  }
  int ver  = rdata.head.ver;
  int type = rdata.head.type;
  int size = rdata.head.size;
  printf("type=%d size=%d ver=%d from=%s:%d\n", type, size, ver, inet_ntoa(addr.addr.in.sin_addr), ntohs(addr.addr.in.sin_port));
  switch(type){
    case MTNCMD_LIST:
      mtnfs_list_process(&sdata, &rdata);
      break;
    case MTNCMD_HELLO:
      mtnfs_hello_process(&sdata, &rdata);
      break;
    case MTNCMD_INFO:
      mtnfs_info_process(&sdata, &rdata);
      break;
  }
  printf("size=%d\n", sdata.head.size);
  send_dgram(s, &sdata, &addr);
}

int mtnfs_child_get(int s, kdata *data)
{
  int r;
  int f;
  kdata sd;
  kdata rd;
  struct stat st;
  uint8_t path[PATH_MAX];
  uint8_t *buff = data->data.data;
  size_t   size = data->head.size;

  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.type = MTNRES_SUCCESS;
  sd.head.size = 0;

  strcpy(path, buff);
  buff += strlen(path) + 1;
  lprintf(0,"get: %s\n", path);

  f = open(path, O_RDONLY);
  while(is_loop){
    sd.head.size = read(f, sd.data.data, sizeof(sd.data.data));
    send_stream(s, &sd);
    if(sd.head.size == 0){
      break;
    }
  }
  close(f);
}

int mtnfs_child_set(int s, kdata *data)
{
  int r;
  int f;
  kdata sd;
  kdata rd;
  struct stat st;
  uint8_t path[PATH_MAX];
  uint8_t *buff = data->data.data;
  size_t size;

  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.type = MTNRES_SUCCESS;
  sd.head.size = 0;

  strcpy(path, buff);
  buff += strlen(path) + 1;
  size = sizeof(mode_t);
  st.st_mode = ntohs(*(mode_t *)buff);
  buff += size;
  size = sizeof(time_t);
  st.st_atime = ntohl(*(time_t *)buff);
  st.st_mtime = ntohl(*(time_t *)buff);
  buff += size;

  lprintf(0,"set: %s\n", path);
  f= creat(path, st.st_mode);
  if(f == -1){
    sd.head.type = MTNRES_ERROR;
    sd.head.size = sizeof(errno);
    memcpy(sd.data.data, &errno, sizeof(errno));
    send_stream(s, &sd);
    exit(0);
  }
  
  while(is_loop){
    r = recv_stream(s, &(rd.head), sizeof(rd.head));
    if(r == -1){
      lprintf(0, "%s: head recv error\n", __func__);
      break;
    }
    if(r == 1){
      lprintf(0, "%s: remote close\n", __func__);
      return(1);
    }
    if(rd.head.size == 0){
      struct timeval tv[2];
      tv[0].tv_sec  = st.st_atime;
      tv[0].tv_usec = 0;
      tv[1].tv_sec  = st.st_mtime;
      tv[1].tv_usec = 0;
      futimes(f, tv);
      close(f);
      break;
    }else{
      r = recv_stream(s, rd.data.data, rd.head.size);
      if(r == -1){
        lprintf(0, "%s: data recv error\n", __func__);
        break;
      }
      if(r == 1){
        lprintf(0, "%s: data recv error(remote close)\n", __func__);
        break;
      }
      r = write(f, rd.data.data, rd.head.size);
      if(r == -1){
        printf("error: %s\n", strerror(errno));
        sd.head.type = MTNRES_ERROR;
        sd.head.size = sizeof(errno);
        memcpy(sd.data.data, &errno, sizeof(errno));
        break;
      }
    }
  }
  send_stream(s, &sd);
  return(0);
}

int mtnfs_child_cmd(int s, kdata *data)
{
  switch(data->head.type){
    case MTNCMD_SET:
      return(mtnfs_child_set(s, data));
    case MTNCMD_GET:
      return(mtnfs_child_get(s, data));
  }
  return(0);
}

void mtnfs_child(int s, kaddr *addr)
{
  int r;
  kdata kd;

  lprintf(0,"%s: PID=%d accept from %s:%d\n", __func__, getpid(), inet_ntoa(addr->addr.in.sin_addr), ntohs(addr->addr.in.sin_port));
  while(is_loop){
    r = recv_stream(s, &(kd.head), sizeof(kd.head));
    if(r == -1){
      lprintf(0, "%s: head recv error\n", __func__);
      break;
    }
    if(r == 1){
      lprintf(0, "%s: remote close\n", __func__);
      break;
    }
    if(kd.head.size){
      r = recv_stream(s, kd.data.data, kd.head.size);
      if(r == -1){
        lprintf(0, "%s: data recv error\n", __func__);
        break;
      }
      if(r == 1){
        lprintf(0, "%s: data recv error(remote close)\n", __func__);
        break;
      }
    }
    lprintf(0,"pid=%d type=%d size=%d\n", getpid(), kd.head.type, kd.head.size);
    if(mtnfs_child_cmd(s, &kd)){
      break;
    }
  }
  lprintf(0, "%s: PID=%d END\n", __func__, getpid());
}

void mtnfs_accept_process(int l)
{
  int s;
  pid_t pid;
  kaddr addr;
  memset(&addr, 0, sizeof(addr));
  addr.len = sizeof(addr.addr);
  s = accept(l, &(addr.addr.addr), &(addr.len));
  if(s == -1){
    lprintf(0, "%s: %s\n", __func__, strerror(errno));
    exit(0);
  }

  pid = fork();
  if(pid == -1){
    lprintf(0, "%s: fork error\n", __func__);
    close(s);
    return;
  }
  if(pid){
    close(s);
    return;
  }

  //----- child process -----
  lprintf(0, "%s: PID=%d CHILD START\n", __func__, getpid());
  close(l);
  mtnfs_child(s, &addr);
  close(s);
  lprintf(0, "%s: PID=%d CHILD END\n", __func__, getpid());
  exit(0);
}

void do_loop(int m, int l)
{
  fd_set fds;
  struct timeval tv;
  while(is_loop){
    tv.tv_sec  = 1;
    tv.tv_usec = 0;
    FD_ZERO(&fds);
    FD_SET(l, &fds);
    FD_SET(m, &fds);
    waitpid(-1, NULL, WNOHANG);
    if(select(1024, &fds, NULL,  NULL, &tv) <= 0){
      continue;
    }
    if(FD_ISSET(m, &fds)){
      mtnfs_udp_process(m);
    }
    if(FD_ISSET(l, &fds)){
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

