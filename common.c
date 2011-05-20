/*
 * common.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"

int is_loop = 1;
koption kopt;
kmember *members = NULL;

void mtn_init_option()
{
  memset((void *)&kopt, 0, sizeof(kopt));
  strcpy(kopt.mcast_addr, "224.0.0.110");	
  kopt.mcast_port = 6000;
  kopt.host[0]    = 0;
  kopt.freesize   = 0;
  kopt.datasize   = 0;
  kopt.debuglevel = 0;
  kopt.max_packet_size = 1024;
}

int send_readywait(int s)
{
  fd_set fds;
  struct timeval tv;
  int sendready = 0;
  while(!sendready){
    FD_ZERO(&fds);
    FD_SET(s,&fds);
    tv.tv_sec  = 1;
    tv.tv_usec = 0;
    if(select(1024, NULL, &fds, NULL, &tv) == 1){
      sendready = FD_ISSET(s, &fds);
    }else{
      if(is_loop){
        continue;
      }else{
        break;
      }
    }
  }
  return(sendready);
}

int recv_readywait(int s)
{
  fd_set fds;
  struct timeval tv;
  int recvready = 0;
  while(!recvready){
    FD_ZERO(&fds);
    FD_SET(s,&fds);
    tv.tv_sec  = 1;
    tv.tv_usec = 0;
    if(select(1024, NULL, &fds, NULL, &tv) == 1){
      recvready = FD_ISSET(s, &fds);
    }else{
      if(is_loop){
        continue;
      }else{
        break;
      }
    }
  }
  return(recvready);
}

int recv_dgram(int s, kdata *data, struct sockaddr *addr, socklen_t *alen)
{
  int r;
  while(is_loop){
    r = recvfrom(s, data, sizeof(kdata), 0, addr, alen);
    if(r >= 0){
      break;
    }
    if(errno == EAGAIN){
      return(-1);
    }
    if(errno == EINTR){
      continue;
    }
    lprintf(0, "[error] %s: %s recv error\n", __func__, strerror(errno));
    return(-1);
  }
  if(r < sizeof(khead)){
    return(-1);
  }
  if(data->head.ver != PROTOCOL_VERSION){
    return(-1);
  }
  data->head.size = ntohs(data->head.size);
  if(r != data->head.size + sizeof(khead)){
    return(-1);
  }  
  return(0);
}

int recv_stream(int s, uint8_t *buff, size_t size)
{
  int r;
  while(size){
    if(recv_readywait(s) == 0){
      return(1);
    }
    r = read(s, buff, size);
    if(r == -1){
      if(errno == EAGAIN){
        lprintf(0, "[warn] %s: %s\n", __func__, strerror(errno));
        continue;
      }
      if(errno == EINTR){
        lprintf(0, "[warn] %s: %s\n", __func__, strerror(errno));
        continue;
      }else{
        lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
        return(-1);
      }
    }
    if(r == 0){
      return(1);
    }
    buff += r;
    size -= r;
  }
  return(0);
}

int recv_stream_line(int s, uint8_t *buff, size_t size)
{
  int r;
  uint8_t data;
  while(recv_readywait(s)){
    r = read(s, &data, 1);
    if(r == -1){
      if(errno == EAGAIN){
        continue;
      }
      if(errno == EINTR){
        continue;
      }else{
        lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
        return(-1);
      }
    }
    if(r == 0){
      break;
    }
    if(data == '\r'){
      continue;
    }
    if(data == '\n'){
      break;
    }
    *buff = data;
    buff++;
    size--;
    if(size == 0){
      lprintf(0, "[error] %s: buffer short\n", __func__);
      return(-1);
    }
  }
  *buff = 0;
  return(0);
}

int send_dgram(int s, kdata *data, kaddr *addr)
{
  kdata sd;
  int size = data->head.size + sizeof(khead);
  memcpy(&sd, data, size);
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = htons(data->head.size);
  while(send_readywait(s)){
    int r = sendto(s, &sd, size, 0, &(addr->addr.addr), addr->len);
    if(r == size){
      return(0); /* success */
    }
    if(r == -1){
      if(errno == EAGAIN){
        continue;
      }
      if(errno == EINTR){
        continue;
      }
    }
    break;
  }
  return(-1);
}

int send_stream(int s, kdata *data)
{
  size_t   size;
  uint8_t *buff;
  size  = sizeof(khead);
  size += data->head.size;
  buff  = (uint8_t *)data;
  while(send_readywait(s)){
    int r = send(s, buff, size, 0);
    if(r == -1){
      if(errno == EINTR){
        continue;
      }
      return(-1);
    }
    if(size == r){
      break;
    }
    size -= r;
    buff += r;
  }
  return(0);
}

void lprintf(int l, char *fmt, ...)
{
  va_list arg;
  struct timeval tv;
  char b[1024];
  char d[2048];
  static char m[2048];
  strcpy(d, m);
  va_start(arg, fmt);
  vsnprintf(b, sizeof(b), fmt, arg);
  va_end(arg);
  b[sizeof(b) - 1] = 0;
  snprintf(m, sizeof(m), "%s%s", d, b);
  m[sizeof(m) - 1] = 0;
  m[sizeof(m) - 2] = '\n';
  if(!strchr(m, '\n')){
    return;
  }
#ifdef KFS_DEBUG
  gettimeofday(&tv, NULL);
  fprintf(stderr, "%02d.%06d %s", tv.tv_sec % 60, tv.tv_usec, m);
#else
  fprintf(stderr, "%s", m);
#endif
  m[0] = 0;
}

int create_socket(int port, int mode)
{
  int s;
  int reuse = 1;
  struct sockaddr_in addr;
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(port);
  addr.sin_addr.s_addr = INADDR_ANY;
  s=socket(AF_INET, mode, 0);
  if(s == -1){
    lprintf(0, "%s: can't create socket\n", __func__);
    return(-1);
  }
  if(setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) == -1){
    lprintf(0, "%s: SO_REUSEADDR error\n", __func__);
    return(-1);
  }
  if(bind(s, (struct sockaddr*)&addr, sizeof(addr)) == -1){
    lprintf(0, "%s: bind error\n", __func__);
    return(-1);
  }
  return(s);
}

int create_lsocket(int port)
{
  int s = create_socket(port, SOCK_STREAM);
  if(s == -1){
    return(-1);
  }
  return(s);
}

int create_msocket(int port)
{
  char lpen = 1;
  char mttl = 1;
  struct ip_mreq mg;
  mg.imr_multiaddr.s_addr = inet_addr("224.0.0.110");
  mg.imr_interface.s_addr = INADDR_ANY;

  int s = create_socket(port, SOCK_DGRAM);
  if(s == -1){
    return(-1);
  }
  if(setsockopt(s, IPPROTO_IP, IP_ADD_MEMBERSHIP, (void *)&mg, sizeof(mg)) == -1){
    lprintf(0, "%s: IP_ADD_MEMBERSHIP error\n", __func__);
    return(-1);
  }
  if(setsockopt(s, IPPROTO_IP, IP_MULTICAST_IF,   (void *)&mg.imr_interface.s_addr, sizeof(mg.imr_interface.s_addr)) == -1){
    lprintf(0, "%s: IP_MULTICAST_IF error\n", __func__);
    return(-1);
  }
  if(setsockopt(s, IPPROTO_IP, IP_MULTICAST_LOOP, (void *)&lpen, sizeof(lpen)) == -1){
    lprintf(0, "%s: IP_MULTICAST_LOOP error\n", __func__);
    return(-1);
  }
  if(setsockopt(s, IPPROTO_IP, IP_MULTICAST_TTL,  (void *)&mttl, sizeof(mttl)) == -1){
    lprintf(0, "%s: IP_MULTICAST_TTL error\n", __func__);
    return(-1);
  }
  return(s);
}

char *mtn_get_v4addr(kaddr *addr)
{
  static char *dummy="0.0.0.0";
  if(!addr){
    return(dummy);
  }
  return(inet_ntoa(addr->addr.in.sin_addr));
}

int mtn_get_v4port(kaddr *addr)
{
  return(ntohs(addr->addr.in.sin_port));
}

int mtn_get_string(uint8_t *str, kdata *kd)
{
  uint16_t len;
  uint16_t size = kd->head.size;
  uint8_t *buff = kd->data.data;
  if(size > MAX_DATASIZE){
    return(-1);
  }
  for(len=0;len<size;len++){
    if(*(buff + len) == 0){
      break;
    }
  }
  if(len == size){
    return(0);
  }
  len++;
  size -= len;
  if(str){
    memcpy(str, buff, len);
    memmove(buff, buff + len, size);
    kd->head.size = size;
  }
  return(len);
}

int mtn_get_int16(uint16_t *val, kdata *kd)
{
  uint16_t len  = sizeof(uint16_t);
  uint16_t size = kd->head.size;
  if(size > MAX_DATASIZE){
    return(-1);
  }
  if(size < len){
    return(-1);
  }
  size -= len;
  *val = ntohs(kd->data.data16);
  memmove(kd->data.data, kd->data.data + len, size);
  kd->head.size = size;
  return(0);
}

int mtn_get_int32(uint32_t *val, kdata *kd)
{
  uint16_t len  = sizeof(uint32_t);
  uint16_t size = kd->head.size;
  if(size > MAX_DATASIZE){
    return(-1);
  }
  if(size < len){
    return(-1);
  }
  size -= len;
  *val = ntohl(kd->data.data32);
  memmove(kd->data.data, kd->data.data + len, size);
  kd->head.size = size;
  return(0);
}

int mtn_get_int64(uint64_t *val, kdata *kd)
{
  uint16_t  len = sizeof(uint64_t);
  uint16_t size = kd->head.size;
  if(size > MAX_DATASIZE){
    return(-1);
  }
  if(size < len){
    return(-1);
  }
  uint32_t *ptr = (uint32_t *)(kd->data.data + kd->head.size);
  uint64_t hval = (uint64_t)(*(ptr + 0));
  uint64_t lval = (uint64_t)(*(ptr + 1));
  *val = (hval << 32) | lval;
  size -= len;
  memmove(kd->data.data, kd->data.data + len, size);
  kd->head.size = size;
  return(0);
}

int mtn_get_int(void *val, kdata *kd, int size)
{
  switch(size){
    case 2:
      return mtn_get_int16(val, kd);
    case 4:
      return mtn_get_int32(val, kd);
    case 8:
      return mtn_get_int64(val, kd);
  }
  return(-1);
}

int mtn_set_string(uint8_t *str, kdata *kd)
{
  uint16_t len;
  if(str == NULL){
    return(-1);
  }
  len = strlen(str) + 1;
  if(kd->head.size + len > MAX_DATASIZE){
    return(-1);
  }
  memcpy(kd->data.data + kd->head.size, str, len);
  kd->head.size += len;
  return(0);
}

int mtn_set_int16(uint16_t *val, kdata *kd)
{
  uint16_t len = sizeof(uint16_t);
  if(kd->head.size + len > MAX_DATASIZE){
    return(-1);
  }
  *(uint16_t *)(kd->data.data + kd->head.size) = htons(*val);
  kd->head.size += len;
  return(0);
}

int mtn_set_int32(uint32_t *val, kdata *kd)
{
  uint16_t len = sizeof(uint32_t);
  if(kd->head.size + len > MAX_DATASIZE){
    return(-1);
  }
  *(uint32_t *)(kd->data.data + kd->head.size) = htonl(*val);
  kd->head.size += len;
  return(0);
}

int mtn_set_int64(uint64_t *val, kdata *kd)
{
  uint32_t hval = (*val) >> 32;
  uint32_t lval = (*val) & 0xFFFFFFFF;
  uint32_t *ptr = (uint32_t *)(kd->data.data + kd->head.size);
  uint16_t  len = sizeof(uint64_t);
  if(kd->head.size + len > MAX_DATASIZE){
    return(-1);
  }
  *(ptr + 0) = htonl(hval);
  *(ptr + 1) = htonl(lval);
  kd->head.size += len;
  return(0);
}

int mtn_set_int(void *val, kdata *kd, int size)
{
  switch(size){
    case 2:
      return mtn_set_int16(val, kd);
    case 4:
      return mtn_set_int32(val, kd);
    case 8:
      return mtn_set_int64(val, kd);
  }
  return(-1);
}

