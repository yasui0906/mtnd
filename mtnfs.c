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

void mtn_stream_init(kstream *stream)
{
  stream->type = stream->data.head.type;
  if(stream->type == MTNCMD_SAVE){
    strcpy(stream->opt.file_name, stream->data.data.data);
    stream->fd = creat(stream->opt.file_name, 0644);
    lprintf(0,"type=%d size=%d file=%s\n", stream->data.head.type, stream->data.head.size, stream->opt.file_name);
  }
}

void mtn_stream_cleanup(kstream *stream)
{
  if(stream->fd){
    close(stream->fd);
    stream->fd = 0;
  }
}

int mtn_stream_exec(kstream *stream)
{
  if(stream->type == MTNCMD_SAVE){
    lprintf(0,"type=%d size=%d file=%s\n", stream->data.head.type, stream->data.head.size, stream->opt.file_name);
    if(stream->data.head.size == 0){
      return(1);
    }
    write(stream->fd, stream->data.data.data, stream->data.head.size);
  }
  return(0);
}

void mtn_stream_process(kstream *stream)
{
  stream->h_size = 0;
  stream->d_size = 0;
  if(stream->type == MTNCMD_NONE){
    mtn_stream_init(stream);
  }else{
    if(mtn_stream_exec(stream)){
      mtn_stream_cleanup(stream);
    }
  }
}

void mtn_tcp_process(kstream *stream, char *buff, int size)
{
  void *ptr;
  int copy_size;
  int need_size;
  while(size){
    need_size = sizeof(khead) - stream->h_size;
    if(need_size > 0){
      ptr = &(stream->data.head) + stream->h_size;
      if(size > need_size){
        copy_size = need_size;
      }else{
        copy_size = size;
      }
      if(copy_size > 0){
        memcpy(ptr, buff, copy_size);
        buff += copy_size;
        size -= copy_size;
        stream->h_size += copy_size;
      }
    }
    need_size = stream->data.head.size - stream->d_size;
    if(need_size > 0){
      ptr = stream->data.data.data + stream->d_size;
      if(size > need_size){
        copy_size = need_size;
      }else{
        copy_size = size;
      }
      if(copy_size > 0){
        memcpy(ptr, buff, copy_size);
        buff += copy_size;
        size -= copy_size;
        stream->d_size += copy_size;
        need_size -= copy_size;
      }
    }
    if(need_size == 0){
      mtn_stream_process(stream);
    }
  }
}

void do_loop()
{
  int i;
  int r;
  int s;

  fd_set    fds;
  kdata     data;
  socklen_t alen;
  struct sockaddr_storage addr_storage;
  struct sockaddr    *addr    = (struct sockaddr    *)&addr_storage;
  struct sockaddr_in *addr_in = (struct sockaddr_in *)&addr_storage;
  struct timeval tv;
  kstream stream[8];

  int msocket = create_msocket(6000);
  int lsocket = create_lsocket(6000);
  
  memset(stream, 0, sizeof(stream));
  if(listen(lsocket, 5) == -1){
    lprintf(0, "%s: listen error\n", __func__);
    exit(1);
  }

  while(is_loop){
    tv.tv_sec  = 1;
    tv.tv_usec = 0;
    FD_ZERO(&fds);
    FD_SET(lsocket, &fds);
    FD_SET(msocket, &fds);
    for(i=0;i<8;i++){
      if(stream[i].socket){
        FD_SET(stream[i].socket, &fds);
      }
    }
    r = select(1024, &fds, NULL,  NULL, &tv);
    if(r == -1){
      continue;
    }
    if(r == 0){
      continue;
    }
    for(i=0;i<8;i++){
      if(!FD_ISSET(stream[i].socket, &fds)){
        continue;
      }
      char buff[32768];
      r = recv(stream[i].socket, buff, sizeof(buff), 0);
      if(r == -1){
        lprintf(0,"error: %s client=%d\n", i, strerror(errno));
        continue;
      }
      if(r == 0){
        lprintf(0,"info: Connection Close client=%d\n", i);
        close(stream[i].socket);
        stream[i].socket = 0;
        continue;
      }
      mtn_tcp_process(&stream[i], buff, r);
    }
    if(FD_ISSET(lsocket, &fds)){
      alen = sizeof(addr);
      s = accept(lsocket, addr, &alen);
      for(i=0;i<8;i++){
        if(stream[i].socket == 0){
          stream[i].socket = s;
          break;
        }
      }
      if(i==8){
        close(s);
        lprintf(0, "%s: accept error max connection\n", __func__);
      }
    }
    if(FD_ISSET(msocket, &fds)){
      alen = sizeof(addr_storage);
      if(recv_dgram(msocket, &data, addr, &alen) == 0){
        uint8_t *buff = data.data.data;
        int ver  = data.head.ver;
        int type = data.head.type; 
        printf("type=%d ver=%d from=%s:%d\n", type, ver, inet_ntoa(addr_in->sin_addr), ntohs(addr_in->sin_port));
        if(type == MTNCMD_LIST){
        }
        if(type == MTNCMD_INFO){
          kinfo *info = &(data.data.info);
          buff += sizeof(kinfo);
          info->free = kopt.diskfree;
          info->host = buff - (uintptr_t)info;
          strcpy(buff, kopt.host);
          buff += strlen(kopt.host) + 1;
        }
        data.head.size = (uintptr_t)(buff - (uintptr_t)(data.data.data));
        send_dgram(msocket, &data, addr);
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

