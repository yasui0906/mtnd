/*
 * mtntool.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"

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
  printf("   -h       --help\n");
  printf("   -V       --version\n");
  printf("   -i       --info\n");
  printf("   -L path  --list=path\n");
  printf("   -s path  --save=path\n");
  printf("   -l path  --load=path\n");
  printf("\n");
}

void mtn_info()
{
  int    r;
  fd_set fds;
  kdata  data;
  kinfo *info = &(data.data.info);
  struct timeval tv;
  socklen_t alen;
  struct sockaddr_storage addr_storage;
  struct sockaddr    *addr    = (struct sockaddr    *)&addr_storage;
  struct sockaddr_in *addr_in = (struct sockaddr_in *)&addr_storage;

  int s= create_socket(0, SOCK_DGRAM);
  addr_in->sin_family      = AF_INET;
  addr_in->sin_port        = htons(kopt.mcast_port);
  addr_in->sin_addr.s_addr = inet_addr(kopt.mcast_addr);
  data.head.ver            = PROTOCOL_VERSION;
  data.head.type           = MTNCMD_INFO;
  data.head.size           = 0;
  send_dgram(s, &data, addr);

  tv.tv_sec  = 1;
  tv.tv_usec = 0;
  while(is_loop){
    FD_ZERO(&fds);
    FD_SET(s, &fds);
    r = select(1024, &fds, NULL,  NULL, &tv);
    tv.tv_sec  = 0;
    tv.tv_usec = 100000;
    if(r == 0){
      break;
    }
    if(r == -1){
      continue; /* error */
    }
    if(FD_ISSET(s, &fds)){
      alen = sizeof(addr_storage);
      if(recv_dgram(s, &data, addr, &alen) == 0){
        info->host += (uintptr_t)info;
        printf("%s (%LuM Free)\n", info->host, info->free / 1024 / 1024);
      }
    }
  }
}

void mtn_list(char *path)
{
}

uint64_t mtn_choose(char *choose_host, struct sockaddr *choose_addr, socklen_t *choose_alen)
{
  int       r;
  int       s;
  uint64_t  free_max;
  fd_set    fds;
  kdata     data;
  char     *buff = data.data.data;
  kinfo    *info = &(data.data.info);
  struct    timeval tv;
  struct    sockaddr_storage addr_storage;
  struct    sockaddr    *addr    = (struct sockaddr    *)&addr_storage;
  struct    sockaddr_in *addr_in = (struct sockaddr_in *)&addr_storage;
  socklen_t alen;

  s= create_socket(0, SOCK_DGRAM);
  if(s == -1){
    lprintf(0, "%s: create_socket error\n", __func__);
    return(0);
  }
  data.head.ver            = PROTOCOL_VERSION;
  data.head.type           = MTNCMD_INFO;
  data.head.size           = 0;
  addr_in->sin_family      = AF_INET;
  addr_in->sin_port        = htons(kopt.mcast_port);
  addr_in->sin_addr.s_addr = inet_addr(kopt.mcast_addr);
  send_dgram(s, &data, addr);

  free_max   = 0;
  tv.tv_sec  = 1;
  tv.tv_usec = 0;
  while(is_loop){
    FD_ZERO(&fds);
    FD_SET(s, &fds);
    r = select(1024, &fds, NULL,  NULL, &tv);
    tv.tv_sec  = 0;
    tv.tv_usec = 100000;
    if(r == 0){
      break;
    }
    if(r == -1){
      continue; /* error */
    }
    if(FD_ISSET(s, &fds)){
      alen = sizeof(addr_storage);
      if(recv_dgram(s, &data, addr, &alen) == 0){
        if(info->free > free_max){
          free_max = info->free;
          info->host += (uintptr_t)info;
          strcpy(choose_host, info->host);
          memcpy(choose_addr, addr, alen);
          *choose_alen = alen;
        }
      }
    }
  }
  close(s);
  return(free_max);
}

int mtn_load(char *load_path, char *file_path)
{
  return(0);
}

int mtn_save(char *save_path, char *file_path)
{
  int r = 0;
  int f = 0;
  int s = 0;
  
  kdata data;
  uint8_t *p;
  uint64_t free_max;
  char buff[1024];
  char host[1024];
  socklen_t  alen;
  struct sockaddr_storage addr_storage;
  struct sockaddr_in *addr_in = (struct sockaddr_in *)&addr_storage;
  struct sockaddr    *addr    = (struct sockaddr    *)&addr_storage;
  struct stat file_stat;

  memset(&file_stat, 0, sizeof(file_stat));
  if(strcmp("-", file_path)){
    f = open(file_path, O_RDONLY);
    if(f == -1){
      printf("error: %s %s\n", strerror(errno), file_path);
      return(1);
    }
    fstat(f, &file_stat);
  }
  free_max = mtn_choose(host, addr, &alen);
  if(free_max == 0){
    printf("error: node not found\n");
    return(1);
  }
  s = create_socket(0, SOCK_STREAM);
  r = connect(s, addr, alen);
  if(r == -1){
    printf("error: %s %s:%d\n", strerror(errno), inet_ntoa(addr_in->sin_addr), ntohs(addr_in->sin_port));
  }else{
    printf("connect: %s %s (%LuM free)\n", host, inet_ntoa(addr_in->sin_addr), free_max / 1024 / 1024);
    data.head.size = 0;
    data.head.type = MTNCMD_SAVE;
    p = data.data.data;
    strcpy(p, save_path);
    data.head.size += strlen(p) + 1;
    send_stream(s, &data);
    while(r = read(f, data.data.data, 1024)){
      if(r == -1){
        printf("error: %s %s\n", strerror(errno), file_path);
        break;
      }
      data.head.size = r;
      data.head.type = MTNCMD_DATA;
      r = send_stream(s, &data);
      if(r == -1){
        printf("error: %s %s\n", strerror(errno), file_path);
        break;
      }
    }
    data.head.size = 0;
    data.head.type = MTNCMD_DATA;
    r = send_stream(s, &data);
  }
  if(f){
    close(f);
  }
  close(s);
  return(r); 
}

int main(int argc, char *argv[])
{
  int r;
  char load_path[1024];
  char save_path[1024];
  char file_path[1024];
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
  opt[2].has_arg = 1;
  opt[2].flag    = NULL;
  opt[2].val     = 'i';

  opt[3].name    = "list";
  opt[3].has_arg = 1;
  opt[3].flag    = NULL;
  opt[3].val     = 'L';

  opt[4].name    = "file";
  opt[4].has_arg = 1;
  opt[4].flag    = NULL;
  opt[4].val     = 'f';

  opt[5].name    = "save";
  opt[5].has_arg = 1;
  opt[5].flag    = NULL;
  opt[5].val     = 's';

  opt[6].name    = "load";
  opt[6].has_arg = 1;
  opt[6].flag    = NULL;
  opt[6].val     = 'l';

  opt[7].name    = NULL;
  opt[7].has_arg = 0;
  opt[7].flag    = NULL;
  opt[7].val     = 0;

  load_path[0]=0;
  save_path[0]=0;
  file_path[0]=0;
  kinit_option();
  while((r = getopt_long(argc, argv, "f:s:l:L:hvi", opt, NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'v':
        version();
        exit(0);

      case 'i':
        mtn_info();
        exit(0);

      case 'L':
        mtn_list(optarg);
        exit(0);

      case 'f':
        strcpy(file_path, optarg);
        break;

      case 's':
        strcpy(save_path, optarg);
        break;

      case 'l':
        strcpy(load_path, optarg);
        break;

      case '?':
        usage();
        exit(1);
    }
  }
  if(strlen(save_path)){
    if(strlen(file_path)){
      r = mtn_save(save_path, file_path);
    }else{
      r = mtn_save(save_path, save_path);
    }
    exit(r);
  }
  if(strlen(load_path)){
    if(strlen(file_path)){
      r = mtn_load(load_path, file_path);
    }else{
      r = mtn_load(load_path, load_path);
    }
    exit(r);
  }
  usage();
  exit(0);
}

