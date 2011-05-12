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
  printf("\n");
}

int mtn_stream_init(kstream *stream)
{
  size_t size;
  uint8_t  *p;
  kdata  data;
  stream->type = stream->data.head.type;
  if(stream->type == MTNCMD_SAVE){
    p = stream->data.data.data;
    size = strlen(p) + 1;
    memcpy(stream->file_name, p, size);
    p += size;
    size = sizeof(mode_t);
    stream->stat.st_mode = ntohs(*(mode_t *)p);
    p += size;
    size = sizeof(time_t);
    stream->stat.st_mtime = ntohl(*(time_t *)p);
    p += size;
    stream->fd = creat(stream->file_name, stream->stat.st_mode);
    if(stream->fd == -1){
      data.head.ver  = PROTOCOL_VERSION;
      data.head.type = MTNRES_ERROR;
      data.head.size = sizeof(errno);
      memcpy(data.data.data, &errno, sizeof(errno));
      send_stream(stream->socket, &data);
      return(-1);
    }
    lprintf(0,"type=%d size=%d file=%s\n", stream->data.head.type, stream->data.head.size, stream->file_name);
  }
  return(0);
}

void mtn_stream_exit(kstream *stream)
{
  stream->type = MTNCMD_NONE;
  if(stream->fd){
    struct timeval tv[2];
    tv[0].tv_sec  = stream->stat.st_atime;
    tv[0].tv_usec = 0;
    tv[1].tv_sec  = stream->stat.st_mtime;
    tv[1].tv_usec = 0;
    futimes(stream->fd, tv);
    close(stream->fd);
    stream->fd = 0;
  }
}

int mtn_stream_exec(kstream *stream)
{
  int r;
  uint8_t *p;
  size_t   l;
  kdata data;
  if(stream->type == MTNCMD_SAVE){
    lprintf(0,"type=%d size=%d file=%s\n", stream->data.head.type, stream->data.head.size, stream->file_name);
    if(stream->data.head.size == 0){
      data.head.ver  = PROTOCOL_VERSION;
      data.head.type = MTNRES_SUCCESS;
      data.head.size = 0;
      send_stream(stream->socket, &data);
      return(1);
    }
    l = stream->data.head.size;
    p = stream->data.data.data;
    while(l){
      r = write(stream->fd, p, l);
      if(r == -1){
        printf("error: %s\n", strerror(errno));
        data.head.ver  = PROTOCOL_VERSION;
        data.head.type = MTNRES_ERROR;
        data.head.size = sizeof(errno);
        memcpy(data.data.data, &errno, sizeof(errno));
        send_stream(stream->socket, &data);
        return(1);
      }
      p += r;
      l -= r;
    }
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
      mtn_stream_exit(stream);
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

  kinit_option();
  kopt.max_packet_size = 1024;

  if(gethostname(kopt.host, sizeof(kopt.host)) == -1){
    kopt.host[0] = 0;
  }
  kopt.diskfree = 0;

  while((r = getopt_long(argc, argv, "hvne:s:", opt, NULL)) != -1){
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

      case 'e':
        if(chdir(optarg) == -1){
          lprintf(0,"error: %s %s\n", strerror(errno), optarg);
          exit(1);
        }
        break;

      case 's':
        break;

      case '?':
        usage();
        exit(1);
    }
  }

  struct statvfs buff;
  memset(&buff, 0, sizeof(buff));

  kopt.cwd = getcwd(NULL, 0);
  if(statvfs(kopt.cwd, &buff) == -1){
    lprintf(0, "error: %s\n", strerror(errno));
    return(1);
  }
  kopt.diskfree = buff.f_bfree * buff.f_bsize / 1024 / 1024;
  kopt.datasize = get_dirsize(kopt.cwd);

  version();
  lprintf(0, "export   : %s\n",     kopt.cwd);
  lprintf(0, "free size: %u[MB]\n", kopt.diskfree);
  lprintf(0, "data size: %u[MB]\n", kopt.datasize);

  if(daemonize){
    do_daemon();
  }else{
    do_loop();
  }
  return(0);
}

