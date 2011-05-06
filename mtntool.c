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

void info()
{
  int r;
  fd_set fds;
  kdata  data;
  kinfo *info = &(data.data.info);
  struct sockaddr_in addr;
  struct timeval tv;

  data.head.size = 0;
  int s= create_socket(0, SOCK_DGRAM);
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(kopt.mcast_port);
  addr.sin_addr.s_addr = inet_addr(kopt.mcast_addr);

  data.head.ver  = PROTOCOL_VERSION;
  data.head.cmd  = MTNCMD_INFO;
  data.head.size = 0;
  send_packet(s, &data, (struct sockaddr *)&addr);

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
      if(recv_packet(s, &data, (struct sockaddr *)&addr) == 0){
        info->host += (uintptr_t)info;
        printf("%s (%LuM Free)\n", info->host, info->free / 1024 / 1024);
      }
    }
  }
}

void list(char *path)
{
  int r;
  fd_set fds;
  kdata  data;
  kinfo *info = &(data.data.info);
  struct sockaddr_in addr;
  struct timeval tv;

  data.head.size = 0;
  int s= create_socket(0, SOCK_DGRAM);
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(kopt.mcast_port);
  addr.sin_addr.s_addr = inet_addr(kopt.mcast_addr);

  char *buff = data.data.data;
  data.head.ver  = PROTOCOL_VERSION;
  data.head.cmd  = MTNCMD_LIST;
  data.head.size = 0;
  send_packet(s, &data, (struct sockaddr *)&addr);

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
      if(recv_packet(s, &data, (struct sockaddr *)&addr) == 0){
        info->host += (uintptr_t)info;
        printf("%s (%LuM Free)\n", info->host, info->free / 1024 / 1024);
      }
    }
  }
}

uint64_t mtn_choose(char *choose_host, struct sockaddr_storage *choose_addr)
{
  int r;
  uint64_t free_max;
  fd_set  fds;
  kdata   data;
  kinfo  *info = &(data.data.info);
  struct  sockaddr_in addr;
  struct  timeval tv;

  data.head.size = 0;
  int s= create_socket(0, SOCK_DGRAM);
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(kopt.mcast_port);
  addr.sin_addr.s_addr = inet_addr(kopt.mcast_addr);

  char *buff = data.data.data;
  data.head.ver  = PROTOCOL_VERSION;
  data.head.cmd  = MTNCMD_INFO;
  data.head.size = 0;
  send_packet(s, &data, (struct sockaddr *)&addr);

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
      if(recv_packet(s, &data, (struct sockaddr *)&addr) == 0){
        if(info->free > free_max){
          free_max = info->free;
          info->host += (uintptr_t)info;
          strcpy(choose_host, info->host);
          memcpy(choose_addr, &addr, sizeof(addr));
        }
      }
    }
  }
  return(free_max);
}

void load(char *path)
{
}

int save(char *save_path, char *file_path)
{
  int r = 0;
  int f = 0;
  int s = 0;
  uint64_t free_max;
  char buff[1024];
  char host[1024];
  struct sockaddr_storage addr;

  if(strcmp("-", file_path)){
    f = open(file_path, O_RDONLY);
    if(f == -1){
      printf("error: %s %s\n", strerror(errno), file_path);
      exit(1);
    }
  }
  free_max = mtn_choose(host, &addr);
  if(free_max == 0){
    printf("error: node not found\n");
    exit(1);
  }
  printf("%s %s (%LuM free)\n", host, inet_ntoa(((struct sockaddr_in *)(&addr))->sin_addr), free_max / 1024 / 1024);
  s = create_socket(0, SOCK_STREAM);
  r = connect(s, (struct sockaddr *)&addr, sizeof(addr));
  if(r == -1){
    printf("error: %s %s\n", strerror(errno), host);
    exit(1);
  }
  exit(0);
  while(r = read(f, buff, 1024)){
    if(r == -1){
      printf("error: %s %s\n", strerror(errno), file_path);
      exit(1);
    }
  } 
}

int main(int argc, char *argv[])
{
  int r;
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
        info();
        exit(0);

      case 'L':
        list(optarg);
        exit(0);

      case 'f':
        strcpy(file_path, optarg);
        break;

      case 's':
        strcpy(save_path, optarg);
        break;

      case 'l':
        load(optarg);
        exit(0);

      case '?':
        usage();
        exit(1);
    }
  }
  if(strlen(save_path)){
    if(strlen(file_path)){
      r = save(save_path, file_path);
    }else{
      r = save(save_path, save_path);
    }
  }else{
    usage();
  }
  exit(0);
}

