/*
 * mtntool.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"

typedef void (*MTNPROCFUNC)(kdata *sdata, kdata *rdata, kaddr *addr);
kmember *make_member(uint8_t *host, kaddr *addr);
kmember *get_member(kaddr *addr, int mkflag);

void version()
{
  printf("%s version %s\n", MODULE_NAME, PACKAGE_VERSION);
}

void usage()
{
  version();
  printf("usage: %s [OPTION] [PATH]\n", MODULE_NAME);
  printf("\n");
  printf("  OPTION\n");
  printf("   -h      --help\n");
  printf("   -V      --version\n");
  printf("   -i      --info\n");
  printf("   -l      --list\n");
  printf("   -s      --set\n");
  printf("   -g      --get\n");
  printf("   -f path --file=path\n");
  printf("\n");
}

void clear_members()
{
  kmember *member;
  while(members){
    member  = members;
    members = members->next;
    if(member->host){
      free(member->host);
    }
    free(member);
  }
}

kmember *make_member(uint8_t *host, kaddr *addr)
{
  kmember *member = get_member(addr, 0);
  if(member == NULL){
    member = malloc(sizeof(kmember));
    memset(member, 0, sizeof(kmember));
    memcpy(&(member->addr), addr, sizeof(kaddr));
    member->next = members;
    members = member;
  }
  if(host){
    int len = strlen(host) + 1;
    if(member->host){
      free(member->host);
    }
    member->host = malloc(len);
    memcpy(member->host, host, len);
  }
  member->mark = 0;
  return(member);
}

kmember *get_member(kaddr *addr, int mkflag)
{
  kmember *member;
  for(member=members;member;member=member->next){
    if(memcmp(addr, &(member->addr), sizeof(kaddr)) == 0){
      return(member);
    }
  }
  if(mkflag){
    return(make_member(NULL, addr));
  }
  return(NULL);
}

void mtn_process(kdata *sdata, MTNPROCFUNC mtn)
{
  int r;
  kaddr addr;
  kdata rdata;
  fd_set fds;
  struct timeval tv;
  kmember *member;

  int s= create_socket(0, SOCK_DGRAM);
  addr.len                     = sizeof(struct sockaddr_in);
  addr.addr.in.sin_family      = AF_INET;
  addr.addr.in.sin_port        = htons(kopt.mcast_port);
  addr.addr.in.sin_addr.s_addr = inet_addr(kopt.mcast_addr);
  sdata->head.ver              = PROTOCOL_VERSION;
  send_dgram(s, sdata, &addr);

  tv.tv_sec  = 5;
  tv.tv_usec = 0;
  while(is_loop){
    FD_ZERO(&fds);
    FD_SET(s, &fds);
    r = select(1024, &fds, NULL,  NULL, &tv);
    if(sdata->head.type == MTNCMD_HELLO){
      tv.tv_sec  = 0;
      tv.tv_usec = 100000;
    }else{
      tv.tv_sec  = 5;
      tv.tv_usec = 0;
    }
    if(r == 0){
      break;
    }
    if(r == -1){
      continue; /* error */
    }
    if(FD_ISSET(s, &fds)){
      memset(&addr, 0, sizeof(addr));
      addr.len = sizeof(addr.addr);
      if(recv_dgram(s, &rdata, &(addr.addr.addr), &(addr.len)) == 0){
        mtn(sdata, &rdata, &addr);
        if(sdata->head.type != MTNCMD_HELLO){
          member = get_member(&addr, 1);
          member->mark = 1;
          for(member=members;member;member=member->next){
            if(member->mark == 0){
              break;
            }
          }
          if(member == NULL){
            tv.tv_sec  = 0;
            tv.tv_usec = 100000;
          }
        }
      }
    }
  }
}

void mtn_hello_process(kdata *sdata, kdata *rdata, kaddr *addr)
{
  uint8_t host[1024];
  if(mtn_get_string(host, rdata) == -1){
    lprintf(0, "%s: mtn get error\n", __func__);
    return;
  }
  make_member(host, addr);
}

void mtn_hello()
{
  kdata data;
  clear_members();
  data.head.type = MTNCMD_HELLO;
  data.head.size = 0;
  mtn_process(&data, (MTNPROCFUNC)mtn_hello_process);
}

void mtn_info_process(kdata *sdata, kdata *rdata, kaddr *addr)
{
  kmember *member = get_member(addr, 1);
  kinfo *info = &(rdata->data.info);
  info->host += (uintptr_t)info;
  member->free = info->free;
  printf("%s (%uM Free)\n", info->host, info->free);
}

void mtn_info()
{
  kdata data;
  data.head.type = MTNCMD_INFO;
  data.head.size = 0;
  mtn_process(&data, (MTNPROCFUNC)mtn_info_process);
}

void rmstat(kstat *kst)
{
  if(kst->next){
    rmstat(kst->next);
    kst->next = NULL;
  }
  if(kst->name){
    free(kst->name);
    kst->name = NULL;
  }
  free(kst);
}

kstat *mkstat(kaddr *addr, kdata *data)
{
  kstat *kst = NULL;
  size_t len = mtn_get_string(NULL, data);
  if(len == -1){
    printf("%s: data error\n", __func__);
    return(NULL);
  }
  if(len == 0){
    return(NULL);
  }
  kst = malloc(sizeof(kstat));
  memset(kst, 0, sizeof(kstat));
  kst->member = get_member(addr, 1);
  kst->name = malloc(len);
  mtn_get_string(kst->name, data);
  mtn_get_int(&(kst->stat.st_mode),  data, sizeof(kst->stat.st_mode));
  mtn_get_int(&(kst->stat.st_size),  data, sizeof(kst->stat.st_size));
  mtn_get_int(&(kst->stat.st_uid),   data, sizeof(kst->stat.st_uid));
  mtn_get_int(&(kst->stat.st_gid),   data, sizeof(kst->stat.st_gid));
  mtn_get_int(&(kst->stat.st_atime), data, sizeof(kst->stat.st_atime));
  mtn_get_int(&(kst->stat.st_mtime), data, sizeof(kst->stat.st_mtime));
  if(kst->next = mkstat(addr, data)){
    kst->next->prev = kst;
  }
  return kst;
}

kstat *mgstat(kstat *krt, kstat *kst)
{
  kstat *st;
  if(krt){
    for(st=krt;st->next;st=st->next);
    if(st->next = kst){
      kst->prev = st;
    }
  }else{
    krt = kst;
  }
  return krt;
}

void mtn_list_process(kdata *sdata, kdata *rdata, kaddr *addr)
{
  kstat *krt = sdata->option;
  kstat *kst = mkstat(addr, rdata);
  sdata->option = mgstat(krt, kst);
}

int mtn_list(char *path)
{
  kstat *kst;
  kdata data;
  uint8_t m[16];
  struct passwd *pw;
  struct group  *gr;
  data.head.type = MTNCMD_LIST;
  data.head.size = 0;
  data.option    = NULL;
  mtn_set_string(path, &data);
  mtn_process(&data, (MTNPROCFUNC)mtn_list_process);
  for(kst = data.option;kst;kst=kst->next){
    pw = getpwuid(kst->stat.st_uid);
    gr = getgrgid(kst->stat.st_gid);
    get_mode_string(m, kst->stat.st_mode);
    printf("%s: %s %s %s %s\n", kst->member->host, m, pw->pw_name, gr->gr_name, kst->name);
  }
}

void mtn_choose_info(kdata *sdata, kdata *rdata, kaddr *addr)
{
  kmember *choose = sdata->option;
  kmember *member = get_member(addr, 1);
  kinfo *info     = &(rdata->data.info);
  member->free    = info->free;
  if(choose == NULL){
    sdata->option = member;
  }else{
    if(info->free > choose->free){
      sdata->option = member;
    }
  }
}

void mtn_choose_list(kdata *sdata, kdata *rdata, kaddr *addr)
{
}

kmember *mtn_choose(char *path)
{
  kdata data;
  data.head.type = MTNCMD_INFO;
  data.head.size = 0;
  data.option    = NULL;
  mtn_process(&data, (MTNPROCFUNC)mtn_choose_info);
  return(data.option);
}

void mtn_find_process(kdata *sdata, kdata *rdata, kaddr *addr)
{
  sdata->option = mgstat(sdata->option, mkstat(addr, rdata));
}

kstat *mtn_find(char *path)
{
  kdata data;
  data.option    = NULL;
  data.head.type = MTNCMD_LIST;
  data.head.size = strlen(path) + 1;
  strcpy(data.data.data, path);
  mtn_process(&data, (MTNPROCFUNC)mtn_find_process);
  return(data.option);
}

int mtn_set_open(char *path, struct stat *st)
{
  int f;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  if(strcmp("-", path) == 0){
    st->st_uid   = getuid();
    st->st_gid   = getgid();
    st->st_mode  = 0640;
    st->st_atime = tv.tv_sec;
    st->st_mtime = tv.tv_sec;
  }else{
    f = open(path, O_RDONLY);
    if(f == -1){
      printf("error: %s %s\n", strerror(errno), path);
      return(-1);
    }
    fstat(f, st);
  }
  return(f);
}

int mtn_set_stat(int s, char *path, struct stat *st)
{
  uint8_t *p;
  size_t   l;
  kdata data;

  data.head.ver  = PROTOCOL_VERSION;
  data.head.size = 0;
  data.head.type = MTNCMD_SET;

  p = data.data.data;
  l = strlen(path) + 1;
  strcpy(p, path);
  data.head.size += l;
  p += l;

  l = sizeof(mode_t);
  *((mode_t *)p) = htons(st->st_mode);
  data.head.size += l;
  p += l;

  l = sizeof(uint32_t);
  *((uint32_t *)p) = htonl((uint32_t)st->st_ctime);
  data.head.size += l;
  p += l;

  *((uint32_t *)p) = htonl((uint32_t)st->st_mtime);
  data.head.size += l;
  p += l;

  send_stream(s, &data);
}

int mtn_set_write(int f, int s)
{
  int r;
  kdata data;

  while(r = read(f, data.data.data, sizeof(data.data.data))){
    if(r == -1){
      printf("error: %s\n", strerror(errno));
      break;
    }
    data.head.ver  = PROTOCOL_VERSION;
    data.head.size = r;
    data.head.type = MTNCMD_DATA;
    r = send_stream(s, &data);
    if(r == -1){
      printf("error: %s\n", strerror(errno));
      break;
    }
  }
  data.head.ver  = PROTOCOL_VERSION;
  data.head.size = 0;
  data.head.type = MTNCMD_DATA;
  r = send_stream(s, &data);
}

int mtn_set_close(int f, int s)
{
  kdata data;
  if(f > 0){
    close(f);
  }
  if(s < 0){
    return(0);
  }
  if(recv_stream(s, &data, sizeof(data.head))){
    printf("error: can't recv result data\n");
  }else{
    if(recv_stream(s, data.data.data, data.head.size)){
      printf("error: can't recv result data\n");
    }else{
      if(data.head.type == MTNRES_ERROR){
        memcpy(&errno, data.data.data, sizeof(errno)); 
        printf("remote error: %s\n", strerror(errno));
      }
    }
  }
  close(s);
  return(0);
}

int mtn_set(char *save_path, char *file_path)
{
  int f = 0;
  int s = 0;
  kdata data;
  kmember *member; 
  struct stat st;

  member = mtn_choose(save_path);
  if(member == NULL){
    printf("error: node not found\n");
    return(1);
  }

  f = mtn_set_open(file_path, &st);
  if(f == -1){
    printf("error: %s\n", __func__);
    return(1);
  }

  s = create_socket(0, SOCK_STREAM);
  if(s == -1){
    printf("error: %s\n", __func__);
    mtn_set_close(f, s);
    return(1);
  }

  if(connect(s, &(member->addr.addr.addr), member->addr.len) == -1){
    printf("error: %s %s:%d\n", strerror(errno), inet_ntoa(member->addr.addr.in.sin_addr), ntohs(member->addr.addr.in.sin_port));
    mtn_set_close(f, s);
    return(1);
  }
  //printf("connect: %s %s (%dM free)\n", member->host, inet_ntoa(member->addr.addr.in.sin_addr), member->free);
  mtn_set_stat(s, save_path, &st);
  mtn_set_write(f, s);
  mtn_set_close(f, s);
  return(0); 
}

int mtn_get_open(char *path, kstat *st)
{
  int f;
  if(strcmp("-", path) == 0){
    f = 0;
  }else{
    f = creat(path, st->stat.st_mode);
    if(f == -1){
      printf("error: %s %s\n", strerror(errno), path);
    }
  }
  return(f);
}

int mtn_get_write(int f, int s, char *path)
{
  int r;
  kdata sd;
  kdata rd;
  uint8_t *buff;
  size_t size;

  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = 0;
  sd.head.type = MTNCMD_GET;
  size = strlen(path) + 1;
  buff = sd.data.data;
  memcpy(buff, path, size);
  sd.head.size += size;
  send_stream(s, &sd);

  sd.head.size = 0;
  sd.head.type = MTNRES_SUCCESS;
  while(is_loop){
    recv_stream(s, &(rd.head), sizeof(rd.head));
    if(rd.head.size == 0){
      break;
    }else{
      recv_stream(s, rd.data.data, rd.head.size);
      write(f, rd.data.data, rd.head.size);
    }
  }
  send_stream(s, &sd);
  return(0);
}

int mtn_get_close(int f, int s)
{
  if(f > 0){
    close(f);
  }
  if(s > 0){
    close(s);
  }
  return(0);
}

int mtn_get(char *path, char *file)
{
  int f = 0;
  int s = 0;
  kstat *st;

  st = mtn_find(path);
  if(st == NULL){
    printf("error: node not found\n");
    return(1);
  }

  s = create_socket(0, SOCK_STREAM);
  if(s == -1){
    printf("error: %s\n", __func__);
    return(1);
  }

  if(connect(s, &(st->member->addr.addr.addr), st->member->addr.len) == -1){
    printf("error: %s %s:%d\n", strerror(errno), inet_ntoa(st->member->addr.addr.in.sin_addr), ntohs(st->member->addr.addr.in.sin_port));
    mtn_get_close(f, s);
    return(1);
  }
  //printf("connect: %s %s %s (%dM free)\n", path, st->member->host, inet_ntoa(st->member->addr.addr.in.sin_addr), st->member->free);
  f = mtn_get_open(file, st);
  if(f == -1){
    printf("error: %s\n", __func__);
    return(1);
  }
  mtn_get_write(f, s, path);
  mtn_get_close(f, s);
  return(0); 
}

int main(int argc, char *argv[])
{
  int r;
  int mode;
  char load_path[PATH_MAX];
  char save_path[PATH_MAX];
  char file_path[PATH_MAX];
  struct option opt[8];
  opt[0].name    = "help";
  opt[0].has_arg = 0;
  opt[0].flag    = NULL;
  opt[0].val     = 'h';

  opt[1].name    = "version";
  opt[1].has_arg = 0;
  opt[1].flag    = NULL;
  opt[1].val     = 'v';

  opt[2].name    = "info";
  opt[2].has_arg = 0;
  opt[2].flag    = NULL;
  opt[2].val     = 'i';

  opt[3].name    = "list";
  opt[3].has_arg = 0;
  opt[3].flag    = NULL;
  opt[3].val     = 'l';

  opt[4].name    = "file";
  opt[4].has_arg = 1;
  opt[4].flag    = NULL;
  opt[4].val     = 'f';

  opt[5].name    = "set";
  opt[5].has_arg = 0;
  opt[5].flag    = NULL;
  opt[5].val     = 's';

  opt[6].name    = "get";
  opt[6].has_arg = 0;
  opt[6].flag    = NULL;
  opt[6].val     = 'g';

  opt[7].name    = NULL;
  opt[7].has_arg = 0;
  opt[7].flag    = NULL;
  opt[7].val     = 0;

  load_path[0]=0;
  save_path[0]=0;
  file_path[0]=0;
  mtn_init_option();

  mode = MTNCMD_NONE;
  while((r = getopt_long(argc, argv, "f:sglhvi", opt, NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'v':
        version();
        exit(0);

      case 'i':
        mode = MTNCMD_INFO;
        break;

      case 'l':
        mode = MTNCMD_LIST;
        break;

      case 's':
        mode = MTNCMD_SET;
        break;

      case 'g':
        mode = MTNCMD_GET;
        break;

      case 'f':
        strcpy(file_path, optarg);
        break;

      case '?':
        usage();
        exit(1);
    }
  }
  mtn_hello();
  if(mode == MTNCMD_INFO){
    mtn_info();
    exit(0);
  }
  if(mode == MTNCMD_LIST){
    r = mtn_list(argv[optind]);
    exit(r);
  }
  if(mode == MTNCMD_SET){
    if(file_path[0]){
      r = mtn_set(argv[optind], file_path);
    }else{
      r = mtn_set(argv[optind], argv[optind]);
    }
    exit(r);

  }
  if(mode == MTNCMD_GET){
    if(file_path[0]){
      r = mtn_get(argv[optind], file_path);
    }else{
      r = mtn_get(argv[optind], argv[optind]);
    }
    exit(r);
  }
  usage();
  exit(0);
}

