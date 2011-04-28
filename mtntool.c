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
  printf("   -L       --list\n");
  printf("   -i       --info\n");
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
        printf("Host: %s\n", info->host);
        printf("Free: %Ld\n", info->free);
      }
    }
  }
}

void list()
{
}

void load(char *path)
{
}

void save(char *path)
{
}

int main(int argc, char *argv[])
{
  int r;
  struct option opt[7];
  opt[0].name    = "help";
  opt[0].has_arg = 0;
  opt[0].flag    = NULL;
  opt[0].val     = 'h';

  opt[1].name    = "version";
  opt[1].has_arg = 0;
  opt[1].flag    = NULL;
  opt[1].val     = 'V';

  opt[2].name    = "info";
  opt[2].has_arg = 1;
  opt[2].flag    = NULL;
  opt[2].val     = 'i';

  opt[3].name    = "list";
  opt[3].has_arg = 1;
  opt[3].flag    = NULL;
  opt[3].val     = 'L';

  opt[4].name    = "save";
  opt[4].has_arg = 1;
  opt[4].flag    = NULL;
  opt[4].val     = 's';

  opt[5].name    = "load";
  opt[5].has_arg = 1;
  opt[5].flag    = NULL;
  opt[5].val     = 'l';

  opt[6].name    = NULL;
  opt[6].has_arg = 0;
  opt[6].flag    = NULL;
  opt[6].val     = 0;

  kinit_option();
  while((r = getopt_long(argc, argv, "s:l:hViL", opt, NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'V':
        version();
        exit(0);

      case 'i':
        info();
        exit(0);

      case 'L':
        list();
        exit(0);

      case 's':
        save(optarg);
        exit(0);

      case 'l':
        load(optarg);
        exit(0);

      case '?':
        usage();
        exit(1);
    }
  }
  usage();
  exit(0);
}

