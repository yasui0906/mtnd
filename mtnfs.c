/*
 * mtnfs.c
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
  printf("usage: %s [OPTION]\n", MODULE_NAME);
  printf("\n");
  printf("  OPTION\n");
  printf("   -h  # help");
  printf("   -v  # version");
  printf("   -n  # no daemon");
  printf("\n");
}

void do_loop()
{
  fd_set  fds;
  kdata  data;
  struct sockaddr_in addr;
  struct timeval tv;

  int msocket = create_msocket(6000);
  int lsocket = create_lsocket(6000);
  if(listen(lsocket, 5) == -1){
    lprintf(0, "%s: listen error\n", __func__);
    exit(1);
  }

  while(is_loop){
    FD_ZERO(&fds);
    FD_SET(msocket, &fds);
    tv.tv_sec  = 1;
    tv.tv_usec = 0;
    if(select(1024, &fds, NULL,  NULL, &tv) > 0){
      if(FD_ISSET(lsocket, &fds)){
      }
      if(FD_ISSET(msocket, &fds)){
        if(recv_packet(msocket, &data, (struct sockaddr *)&addr) == 0){
          uint8_t *buff = data.data.data;
          int ver = data.head.ver;
          int cmd = data.head.cmd; 
          printf("cmd=%d ver=%d from=%s:%d\n", cmd, ver, inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));
          if(cmd == MTNCMD_LIST){
          }
          if(cmd == MTNCMD_INFO){
            kinfo *info = &(data.data.info);
            buff += sizeof(kinfo);
            info->free = kopt.diskfree;
            info->host = buff - (uintptr_t)info;
            strcpy(buff, kopt.host);
            buff += strlen(kopt.host) + 1;
          }
          data.head.size = (uintptr_t)(buff - (uintptr_t)(data.data.data));
          send_packet(msocket, &data, (struct sockaddr*)&addr);
        }
      }
    }
  }
}

void do_daemon()
{
}

int main(int argc, char *argv[])
{
  int r;
  int daemonize = 1; 
  struct option opt[8];
  opt[0].name    = "help";
  opt[0].has_arg = 0;
  opt[0].flag    = NULL;
  opt[0].val     = 'h';

  opt[1].name    = "version";
  opt[1].has_arg = 0;
  opt[1].flag    = NULL;
  opt[1].val     = 'V';

  opt[2].name    = "status";
  opt[2].has_arg = 1;
  opt[2].flag    = NULL;
  opt[2].val     = 'S';

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

  opt[6].name    = "export";
  opt[6].has_arg = 1;
  opt[6].flag    = NULL;
  opt[6].val     = 'e';

  opt[7].name    = NULL;
  opt[7].has_arg = 0;
  opt[7].flag    = NULL;
  opt[7].val     = 0;

  kinit_option();
  kopt.max_packet_size = 1024;

  if(gethostname(kopt.host, sizeof(kopt.host)) == -1){
    kopt.host[0] = 0;
  }
  kopt.diskfree = 0;

  while((r = getopt_long(argc, argv, "hVne:", opt, NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'V':
        version();
        exit(0);

      case 'n':
        daemonize = 0;
        break;

      case 'e':
        if(chdir(optarg) == -1){
          lprintf(0,"error: %s %s\n", strerror(errno), optarg);
          exit(1);
        }
        break;

      case '?':
        usage();
        exit(1);
    }
  }

  /* ディスクの空き容量などを取得する */
  struct statvfs buff;
  statvfs(".", &buff);
  kopt.diskfree = buff.f_bfree * buff.f_bsize;
  kopt.datasize = 0; 

  if(daemonize){
    do_daemon();
  }else{
    do_loop();
  }
  return(0);
}

