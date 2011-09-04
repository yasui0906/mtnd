/*
 * libmtnfs.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"
#include "common.h"
typedef void (*MTNPROCFUNC)(kmember *member, kdata *send, kdata *recv, kaddr *addr);
void delstats(kstat *kst);
void delmembers(kmember *members);
kmember *make_member(kmember *members, uint8_t *host, kaddr *addr);
kmember *get_member(kmember *members, kaddr *addr, int mkflag);
kmember *mtn_hello();

int is_loop = 1;
koption kopt;

void mtn_init_option()
{
  memset((void *)&kopt, 0, sizeof(kopt));
  strcpy(kopt.mcast_addr,     "224.0.0.110");
  strcpy(kopt.mtnstatus_path, "/.mtnstatus");
  kopt.mcast_port = 6000;
  kopt.host[0]    = 0;
  kopt.debuglevel = 0;
  kopt.max_packet_size = 1024;
  pthread_mutex_init(&(kopt.cache_mutex),  NULL);
  pthread_mutex_init(&(kopt.member_mutex), NULL);
  pthread_mutex_init(&(kopt.status_mutex), NULL);
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

int recv_stream(int s, void *buff, size_t size)
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

int recv_data_stream(int s, kdata *kd)
{
  int r;
  if(r = recv_stream(s, &(kd->head), sizeof(kd->head))){
    return(r);
  }
  if(kd->head.size > MAX_DATASIZE){
    return(-1);
  }
  return(recv_stream(s, &(kd->data), kd->head.size));
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

int send_data_stream(int s, kdata *data)
{
  size_t   size;
  uint8_t *buff;
  size  = sizeof(khead);
  size += data->head.size;
  buff  = (uint8_t *)data;
  //lprintf(0, "%s: [info] size=%d\n", __func__, size);
  while(send_readywait(s)){
    int r = send(s, buff, size, 0);
    if(r == -1){
      if(errno == EINTR){
        continue;
      }
      lprintf(0, "%s: [error] send error %s\n", __func__, strerror(errno));
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

int send_recv_stream(int s, kdata *sd, kdata *rd)
{
  if(send_data_stream(s, sd) == -1){
    lprintf(0, "[error] %s: send error %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(recv_data_stream(s, rd) == -1){
    lprintf(0, "[error] %s: recv error %s\n", __func__, strerror(errno));
    return(-1);
  }
  return(0);
}

int create_socket(int port, int mode)
{
  int s;
  int reuse = 1;
  struct sockaddr_in addr;
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(port);
  addr.sin_addr.s_addr = INADDR_ANY;
  s = socket(AF_INET, mode, 0);
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
  if(kd->head.size = size){
    memmove(kd->data.data, kd->data.data + len, size);
  }
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
  if(kd->head.size = size){
    memmove(kd->data.data, kd->data.data + len, size);
  }
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
  size -= len;
  uint32_t *ptr = (uint32_t *)(kd->data.data);
  uint64_t hval = (uint64_t)(ntohl(*(ptr + 0)));
  uint64_t lval = (uint64_t)(ntohl(*(ptr + 1)));
  *val = (hval << 32) | lval;
  if(kd->head.size = size){
    memmove(kd->data.data, kd->data.data + len, size);
  }
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
    *(kd->data.data + kd->head.size) = 0;
    kd->head.size++;
    return(0);
  }
  len = strlen(str) + 1;
  if(kd){
    if(kd->head.size + len > MAX_DATASIZE){
      return(-1);
    }
    memcpy(kd->data.data + kd->head.size, str, len);
    kd->head.size += len;
  }
  return(len);
}

int mtn_set_int16(uint16_t *val, kdata *kd)
{
  uint16_t len = sizeof(uint16_t);
  if(kd){
    if(kd->head.size + len > MAX_DATASIZE){
      return(-1);
    }
    *(uint16_t *)(kd->data.data + kd->head.size) = htons(*val);
    kd->head.size += len;
  }
  return(len);
}

int mtn_set_int32(uint32_t *val, kdata *kd)
{
  uint16_t len = sizeof(uint32_t);
  if(kd){
    if(kd->head.size + len > MAX_DATASIZE){
      return(-1);
    }
    *(uint32_t *)(kd->data.data + kd->head.size) = htonl(*val);
    kd->head.size += len;
  }
  return(len);
}

int mtn_set_int64(uint64_t *val, kdata *kd)
{
  uint16_t  len = sizeof(uint64_t);
  uint32_t hval = (*val) >> 32;
  uint32_t lval = (*val) & 0xFFFFFFFF;
  uint32_t *ptr = NULL;
  if(kd){
    ptr = (uint32_t *)(kd->data.data + kd->head.size);
    if(kd->head.size + len > MAX_DATASIZE){
      return(-1);
    }
    *(ptr + 0) = htonl(hval);
    *(ptr + 1) = htonl(lval);
    kd->head.size += len;
  }
  return(len);
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

int mtn_set_data(void *buff, kdata *kd, size_t size)
{
  size_t max = MAX_DATASIZE - kd->head.size;
  if(MAX_DATASIZE < kd->head.size){
    return(0);
  }
  if(size > max){
    size = max;
  }
  memcpy(kd->data.data + kd->head.size, buff, size);
  kd->head.size += size;
  return(size);
}

int mtn_set_stat(struct stat *st, kdata *kd)
{
  int r = 0;
  int l = 0;
  if(st){
    r = mtn_set_int(&(st->st_mode),  kd, sizeof(st->st_mode));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtn_set_int(&(st->st_size),  kd, sizeof(st->st_size));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtn_set_int(&(st->st_uid),   kd, sizeof(st->st_uid));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtn_set_int(&(st->st_gid),   kd, sizeof(st->st_gid));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtn_set_int(&(st->st_atime), kd, sizeof(st->st_atime));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtn_set_int(&(st->st_mtime), kd, sizeof(st->st_mtime));
    if(r == -1){return(-1);}else{l+=r;}
  }
  return(l);
}

int mtn_get_stat(struct stat *st, kdata *kd)
{
  if(st && kd){
    if(mtn_get_int(&(st->st_mode),  kd, sizeof(st->st_mode)) == -1){
      return(-1);
    }
    if(mtn_get_int(&(st->st_size),  kd, sizeof(st->st_size)) == -1){
      return(-1);
    }
    if(mtn_get_int(&(st->st_uid),   kd, sizeof(st->st_uid)) == -1){
      return(-1);
    }
    if(mtn_get_int(&(st->st_gid),   kd, sizeof(st->st_gid)) == -1){
      return(-1);
    }
    if(mtn_get_int(&(st->st_atime), kd, sizeof(st->st_atime)) == -1){
      return(-1);
    }
    if(mtn_get_int(&(st->st_mtime), kd, sizeof(st->st_mtime)) == -1){
      return(-1);
    }
    return(0);
  }
  return(-1);
}

void get_mode_string(uint8_t *buff, mode_t mode)
{
  uint8_t *perm[] = {"---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx"};
  if(S_ISREG(mode)){
    *(buff++) = '-';
  }else if(S_ISDIR(mode)){
    *(buff++) = 'd';
  }else if(S_ISCHR(mode)){
    *(buff++) = 'c';
  }else if(S_ISBLK(mode)){
    *(buff++) = 'b';
  }else if(S_ISFIFO(mode)){
    *(buff++) = 'p';
  }else if(S_ISLNK(mode)){
    *(buff++) = 'l';
  }else if(S_ISSOCK(mode)){
    *(buff++) = 's';
  }
  int m;
  m = (mode >> 6) & 7;
  strcpy(buff, perm[m]);
  buff += 3;
  m = (mode >> 3) & 7;
  strcpy(buff, perm[m]);
  buff += 3;
  m = (mode >> 0) & 7;
  strcpy(buff, perm[m]);
  buff += 3;
  *buff = 0;
}

//----------------------------------------------------------------
// new
//----------------------------------------------------------------
kdir *newkdir(const char *path)
{
  kdir *kd;
  kd = malloc(sizeof(kdir));
  memset(kd,0,sizeof(kdir));
  strcpy(kd->path, path);
  kcount(1,0,0);
  return(kd);
}

kstat *newstat(const char *name)
{
  char b[PATH_MAX];
  char f[PATH_MAX];
  kstat *kst = malloc(sizeof(kstat));
  memset(kst, 0, sizeof(kstat));
  if(!name){
    b[0] = 0;
  }else{
    strcpy(b, name);
  }
  strcpy(f, basename(b));
  kst->name = malloc(strlen(f) + 1);
  strcpy(kst->name, f);
  kcount(0,1,0);
  return(kst);
}

kmember *newmember()
{
  kmember *km;
  km = malloc(sizeof(kmember));
  memset(km,0,sizeof(kmember));
  kcount(0,0,1);
  return(km);
}

//----------------------------------------------------------------
// del
//----------------------------------------------------------------
kdir *deldir(kdir *kd)
{
	kdir *n;
  if(!kd){
    return;
  }
  delstats(kd->kst);
  kd->kst = NULL;
  if(kd->prev){
    kd->prev->next = kd->next;
  }
  if(kd->next){
    kd->next->prev = kd->prev;
  }
	n = kd->next;
  free(kd);
  kcount(-1,0,0);
	return(n);
}

kstat *delstat(kstat *kst)
{
	kstat *r = NULL;
  if(!kst){
    return NULL;
  }
  if(kst->prev){
		kst->prev->next = kst->next;
		kst->prev = NULL;
	}
  if(kst->next){
		r = kst->next;
		kst->next->prev = kst->prev;
    kst->next = NULL;
  }
  if(kst->name){
    free(kst->name);
    kst->name = NULL;
  }
  delmembers(kst->member);
  free(kst);
  kcount(0, -1, 0);
	return(r);
}

kmember *delmember(kmember *km)
{
  kmember *nm;
  if(km->host){
    free(km->host);
    km->host = NULL;
  }
  nm = km->next;
  free(km);
  kcount(0, 0, -1);
  return(nm);
}

void delstats(kstat *kst)
{
	while(kst){
		kst = delstat(kst);
	}
}

void delmembers(kmember *members)
{
  while(members){
    members = delmember(members);
  }
}

//----------------------------------------------------------------
// make 
//----------------------------------------------------------------
kmember *make_member(kmember *members, uint8_t *host, kaddr *addr)
{
  kmember *member = get_member(members, addr, 0);
  if(member == NULL){
    member = newmember();
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
  return(members);
}

kmember *copy_member(kmember *km)
{
  kmember *nkm;
  if(km == NULL){
    return(NULL);
  }
  nkm = make_member(NULL, km->host, &(km->addr));
  nkm->mark = km->mark;
  nkm->free = km->free;
  return(nkm);
}

kmember *get_member(kmember *members, kaddr *addr, int mkflag)
{
  kmember *member;
  for(member=members;member;member=member->next){
    if(memcmp(addr, &(member->addr), sizeof(kaddr)) == 0){
      return(member);
    }
  }
  if(mkflag){
    return(make_member(members, NULL, addr));
  }
  return(NULL);
}

kmember *get_members(){
  struct timeval tv;
  kmember *member  = NULL;
  kmember *members = NULL;
  pthread_mutex_lock(&(kopt.member_mutex));
  gettimeofday(&tv, NULL);
  if(tv.tv_sec - 10 > kopt.member_tv.tv_sec){
    delmembers(kopt.members);
    kopt.members = NULL;
  }
  if(kopt.members == NULL){
    if(tv.tv_sec - 10 > kopt.member_tv.tv_sec){
      kopt.members = mtn_hello();
      memcpy(&(kopt.member_tv), &tv, sizeof(struct timeval));
    }
  }
  for(member=kopt.members;member;member=member->next){
    members = make_member(members, member->host, &(member->addr));
  }
  pthread_mutex_unlock(&(kopt.member_mutex));
  return(members);
}

//-------------------------------------------------------------------
//
//
//
//-------------------------------------------------------------------
void mtn_process(kmember *members, kdata *sdata, MTNPROCFUNC mtn)
{
  int r;
  int e;
  int s;
  int t;
  kaddr addr;
  kdata rdata;
  kmember *mb;
  struct epoll_event ev;

  if((members == NULL) && (sdata->head.type != MTNCMD_HELLO)){
    return;
  }
  e = epoll_create(1);
  if(e == -1){
    lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }
  s = create_socket(0, SOCK_DGRAM);
  if(s == -1){
    close(e);
    lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }
  ev.events = EPOLLIN;
  if(epoll_ctl(e, EPOLL_CTL_ADD, s, &ev) == -1){
    lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }
  addr.len                     = sizeof(struct sockaddr_in);
  addr.addr.in.sin_family      = AF_INET;
  addr.addr.in.sin_port        = htons(kopt.mcast_port);
  addr.addr.in.sin_addr.s_addr = inet_addr(kopt.mcast_addr);
  sdata->head.ver              = PROTOCOL_VERSION;
  send_dgram(s, sdata, &addr);

  t = 3000;
  while(is_loop){
    r = epoll_wait(e, &ev, 1, t);
    if(r == 0){
      break;
    }
    if(r == -1){
      continue;
    }
    memset(&addr, 0, sizeof(addr));
    addr.len = sizeof(addr.addr);
    if(recv_dgram(s, &rdata, &(addr.addr.addr), &(addr.len))){
      continue;
    }
    if(!members){
      mtn(NULL, sdata, &rdata, &addr);
      t = 100;
      continue;
    }
    mb = get_member(members, &addr, 1);
    mtn(mb, sdata, &rdata, &addr);
    if(rdata.head.fin == 0){
      continue;
    }
    t = 10;
    mb->mark = 1;
    for(mb=members;mb;mb=mb->next){
      if(mb->mark == 0){
        t = 3000;
        break;
      }
    }
  }
  close(e);
  close(s);
}

void mtn_hello_process(kmember *member, kdata *sdata, kdata *rdata, kaddr *addr)
{
  uint8_t host[1024];
  kmember *members = (kmember *)(sdata->option);
  if(mtn_get_string(host, rdata) == -1){
    lprintf(0, "%s: mtn get error\n", __func__);
    return;
  }
  sdata->option = make_member(members, host, addr);
}

kmember *mtn_hello()
{
  kdata data;
  kmember *members;
  data.head.type = MTNCMD_HELLO;
  data.head.size = 0;
  data.option = NULL;
  mtn_process(NULL, &data, (MTNPROCFUNC)mtn_hello_process);
  return((kmember *)(data.option));
}

void mtn_info_process(kmember *member, kdata *sdata, kdata *rdata, kaddr *addr)
{
  mtn_get_int(&(member->free), rdata, sizeof(member->free));
}

kmember *mtn_info()
{
  kdata data;
  kmember *members = get_members();
  data.head.type   = MTNCMD_INFO;
  data.head.size   = 0;
  mtn_process(members, &data, (MTNPROCFUNC)mtn_info_process);
  return(members);
}

kstat *mkstat(kmember *member, kaddr *addr, kdata *data)
{
  char bf[PATH_MAX];
  struct passwd *pw;
  struct group  *gr;
  kstat *kst = NULL;
  size_t len = mtn_get_string(NULL, data);
  if(len == -1){
    lprintf(0,"%s: data error\n", __func__);
    return(NULL);
  }
  if(len == 0){
    return(NULL);
  }
  kst = newstat(NULL);
  kst->name = malloc(len);
  mtn_get_string(kst->name,  data);
  mtn_get_stat(&(kst->stat), data);
  kst->member = copy_member(member);

	if(kst->member->host){
		len = strlen(kst->member->host);
		if(kopt.field_size[0] < len){
			kopt.field_size[0] = len;
		}
	}

  if(pw = getpwuid(kst->stat.st_uid)){
    strcpy(bf, pw->pw_name);
  }else{
    sprintf(bf, "%d", kst->stat.st_uid);
  }  
  len = strlen(bf);
  if(kopt.field_size[1] < len){
    kopt.field_size[1] = len;
  }

  if(gr = getgrgid(kst->stat.st_gid)){
    strcpy(bf, gr->gr_name);
  }else{
    sprintf(bf, "%d", kst->stat.st_uid);
  }  
  len = strlen(bf);
  if(kopt.field_size[2] < len){
    kopt.field_size[2] = len;
  }

  sprintf(bf, "%llu", kst->stat.st_size);
  len = strlen(bf);
  if(kopt.field_size[3] < len){
    kopt.field_size[3] = len;
  }
  if(kst->next = mkstat(member, addr, data)){
    kst->next->prev = kst;
  }
  return kst;
}

kstat *mgstat(kstat *krt, kstat *kst)
{
  kstat *st;
  kstat *rt;
  if(!krt){
    return(kst);
  }
  rt = krt;
  while(rt){
    st = kst;
    while(st){
      if(strcmp(rt->name, st->name) == 0){
        if(rt->stat.st_mtime < st->stat.st_mtime){
          memcpy(&(rt->stat), &(st->stat), sizeof(struct stat));
          delmembers(rt->member);
          rt->member = st->member;
          st->member = NULL;
        }
        if(st == kst){
          kst = delstat(st);
          st = kst;
        }else{
          st = delstat(st);
        }
        continue;
      }
      st = st->next;
    }
    rt = rt->next;
  }
  if(kst){
    for(rt=krt;rt->next;rt=rt->next);
    if(rt->next = kst){
      kst->prev = rt;
    }
  }
  return(krt);
}

kstat *copy_stats(kstat *kst)
{
  kstat *ks = NULL;
  kstat *kr = NULL;
  while(kst){
    ks = newstat(kst->name);
    memcpy(&(ks->stat), &(kst->stat), sizeof(struct stat));
    ks->member = copy_member(kst->member);
    if(ks->next = kr){
      kr->prev = ks;
    }
    kr = ks;
    kst=kst->next;
  }
  return(kr);
}

void addstat_dircache(const char *path, kstat *kst)
{
  kdir *kd;
  if(kst == NULL){
    return;
  }
  pthread_mutex_lock(&(kopt.cache_mutex));
  kd = kopt.dircache;
  while(kd){
    if(strcmp(kd->path, path) == 0){
      break;
    }
    kd = kd->next;
  }
  if(kd == NULL){
    kd = newkdir(path);
    if(kd->next = kopt.dircache){
      kopt.dircache->prev = kd;
    }
    kopt.dircache = kd;
  }
  if(kst->next = kd->kst){
    kd->kst->prev = kst;
  }
  kd->kst = kst; 
  gettimeofday(&(kd->tv), NULL);
  pthread_mutex_unlock(&(kopt.cache_mutex));
}

void setstat_dircache(const char *path, kstat *kst)
{
  kdir *kd;
  pthread_mutex_lock(&(kopt.cache_mutex));
  kd = kopt.dircache;
  while(kd){
    if(strcmp(kd->path, path) == 0){
      break;
    }
    kd = kd->next;
  }
  if(kd == NULL){
    kd = newkdir(path);
    if(kd->next = kopt.dircache){
      kopt.dircache->prev = kd;
    }
    kopt.dircache = kd;
  }
  delstats(kd->kst);
  kd->kst  = kst;
  kd->flag = 1;
  gettimeofday(&(kd->tv), NULL);
  pthread_mutex_unlock(&(kopt.cache_mutex));
}

kstat *get_dircache(const char *path, int flag)
{
  kdir  *kd;
  kstat *kr;
  struct timeval tv;
  pthread_mutex_lock(&(kopt.cache_mutex));
  gettimeofday(&tv, NULL);
  kd = kopt.dircache;
  while(kd){
    if(kd->tv.tv_sec < tv.tv_sec - 5){
      delstats(kd->kst);
      kd->kst  = NULL;
      kd->flag = 0;
    }
    if(kd->tv.tv_sec < tv.tv_sec - 300){
      if(kd == kopt.dircache){
        kopt.dircache = deldir(kd);
        kd = kopt.dircache;
      }else{
        kd = deldir(kd);
      }
      continue;
    }
    if(strcmp(kd->path, path) == 0){
      break;
    }
    kd = kd->next;
  }
  if(kd == NULL){
    kd = newkdir(path);
    if(kd->next = kopt.dircache){
      kopt.dircache->prev = kd;
    }
    kopt.dircache = kd;
  }
  kr = (!flag || kd->flag) ? copy_stats(kd->kst) : NULL;
  pthread_mutex_unlock(&(kopt.cache_mutex));
  return(kr);
}

//----------------------------------------------------------------------------
//
//
//
//----------------------------------------------------------------------------
void mtn_list_process(kmember *member, kdata *sdata, kdata *rdata, kaddr *addr)
{
  kstat *krt = sdata->option;
  kstat *kst = mkstat(member, addr, rdata);
  sdata->option = mgstat(krt, kst);
}

kstat *mtn_list(const char *path)
{
  kdata data;
  kmember *members = get_members();
  data.head.type   = MTNCMD_LIST;
  data.head.size   = 0;
  data.option      = NULL;
  mtn_set_string((uint8_t *)path, &data);
  kopt.field_size[0] = 1;
  kopt.field_size[1] = 1;
  kopt.field_size[2] = 1;
  kopt.field_size[3] = 1;
  mtn_process(members, &data, (MTNPROCFUNC)mtn_list_process);
  delmembers(members);
  return(data.option);
}

void mtn_stat_process(kmember *member, kdata *sd, kdata *rd, kaddr *addr)
{
  kstat *krt = sd->option;
	if(rd->head.type == MTNRES_SUCCESS){
		kstat *kst = mkstat(member, addr, rd);
		sd->option = mgstat(krt, kst);
	}
}

kstat *mtn_stat(const char *path)
{
	lprintf(0,"[debug] %s: CALL\n", __func__);
  kdata sd;
  kmember *members = get_members();
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_STAT;
  sd.head.size = 0;
  sd.option    = NULL;
  mtn_set_string((uint8_t *)path, &sd);
  mtn_process(members, &sd, (MTNPROCFUNC)mtn_stat_process);
  delmembers(members);
	lprintf(0,"[debug] %s: EXIT\n", __func__);
  return(sd.option);
}

void mtn_choose_info(kmember *member, kdata *sdata, kdata *rdata, kaddr *addr)
{
  kmember *choose = sdata->option;
  mtn_get_int(&(member->free), rdata, sizeof(member->free));
  if(choose == NULL){
    sdata->option = member;
  }else{
    if(member->free > choose->free){
      sdata->option = member;
    }
  }
}

void mtn_choose_list(kdata *sdata, kdata *rdata, kaddr *addr)
{
}

kmember *mtn_choose(const char *path)
{
  kdata data;
  kmember *member;
  kmember *members = get_members();
  data.head.type = MTNCMD_INFO;
  data.head.size = 0;
  data.option    = NULL;
  mtn_process(members, &data, (MTNPROCFUNC)mtn_choose_info);
  member = copy_member(data.option);
  delmembers(members);
  return(member);
}

void mtn_find_process(kmember *member, kdata *sdata, kdata *rdata, kaddr *addr)
{
  sdata->option = mgstat(sdata->option, mkstat(member, addr, rdata));
}

kstat *mtn_find(const char *path, int create_flag)
{
  kstat *kst;
  kdata data;
  kmember *member;
  kmember *members = mtn_info();
  data.option      = NULL;
  data.head.type   = MTNCMD_LIST;
  data.head.size   = 0;
  mtn_set_string((uint8_t *)path, &data);
  mtn_process(members, &data, (MTNPROCFUNC)mtn_find_process);
  kst = data.option;
  if(kst == NULL){
    if(create_flag){
      if(member = mtn_choose(path)){
        kst = newstat(path);
        kst->member = member;
      }
    }
  }
  delmembers(members);
  return(kst);
}

void mtn_mkdir_process(kmember *member, kdata *sd, kdata *rd, kaddr *addr)
{
	if(rd->head.type == MTNRES_ERROR){
    mtn_get_int(&errno, rd, sizeof(errno));
	  lprintf(0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
	}
}

int mtn_mkdir(const char *path)
{
  kdata sd;
  kmember *members = get_members();
	memset(&sd, 0, sizeof(sd));
  if(strlen(path) > strlen(kopt.mtnstatus_path)){
    if(memcmp(kopt.mtnstatus_path, path, strlen(kopt.mtnstatus_path)) == 0){
      return(-EACCES);
    }
  }
  sd.head.type = MTNCMD_MKDIR;
  sd.head.size = 0;
  sd.option    = NULL;
  mtn_set_string((uint8_t *)path, &sd);
  mtn_process(members, &sd, (MTNPROCFUNC)mtn_mkdir_process);
  delmembers(members);
  return(0);
}

void mtn_rm_process(kmember *member, kdata *sd, kdata *rd, kaddr *addr)
{
	if(rd->head.type == MTNRES_ERROR){
    mtn_get_int(&errno, rd, sizeof(errno));
	  lprintf(0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
	}
}

int mtn_rm(const char *path)
{
	lprintf(0,"[debug] %s: CALL\n", __func__);
  kdata sd;
  kmember *members = get_members();
	memset(&sd, 0, sizeof(sd));
  if(strlen(path) > strlen(kopt.mtnstatus_path)){
    if(memcmp(kopt.mtnstatus_path, path, strlen(kopt.mtnstatus_path)) == 0){
      return(-EACCES);
    }
  }
  sd.head.type = MTNCMD_UNLINK;
  sd.head.size = 0;
  sd.option    = NULL;
  mtn_set_string((uint8_t *)path, &sd);
  mtn_process(members, &sd, (MTNPROCFUNC)mtn_rm_process);
  delmembers(members);
	lprintf(0,"[debug] %s: EXIT\n", __func__);
  return(0);
}

int mtn_connect(const char *path, int create_flag)
{
  int s;
  kstat *st = mtn_find(path, create_flag);
  if(st == NULL){
    lprintf(0, "[error] %s: node not found\n", __func__);
    errno = EACCES;
    return(-1);
  }
  s = create_socket(0, SOCK_STREAM);
  if(s == -1){
    lprintf(0, "[error] %s:\n", __func__);
    errno = EACCES;
    return(-1);
  }
  if(connect(s, &(st->member->addr.addr.addr), st->member->addr.len) == -1){
    lprintf(0, "[error] %s: %s %s:%d\n", __func__, strerror(errno), inet_ntoa(st->member->addr.addr.in.sin_addr), ntohs(st->member->addr.addr.in.sin_port));
    close(s);
    errno = EACCES;
    return(-1);
  }
  lprintf(0, "[debug] %s: connect %s %s %s (%llu free)\n", __func__, path, st->member->host, inet_ntoa(st->member->addr.addr.in.sin_addr), st->member->free);
  return(s);
}

//-------------------------------------------------------------------
//
//-------------------------------------------------------------------
int mtn_open(const char *path, int flags, mode_t mode)
{
  kdata sd;
  kdata rd;
  int s = mtn_connect(path, ((flags & O_CREAT) != 0));
  if(s == -1){
    return(-1);
  }
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = 0;
  sd.head.type = MTNCMD_OPEN;
  mtn_set_string((uint8_t *)path, &sd);
  mtn_set_int(&flags, &sd, sizeof(flags));
  mtn_set_int(&mode,  &sd, sizeof(mode));
  if(send_data_stream(s, &sd) == -1){
    close(s);
    return(-1);
  }
  if(recv_data_stream(s, &rd) == -1){
    close(s);
    return(-1);
  }
  if(rd.head.type == MTNRES_ERROR){
    mtn_get_int(&errno, &rd, sizeof(errno));
    close(s);
    return(-1);
  }
  return(s);
}

int mtn_read(int s, char *buf, size_t size, off_t offset)
{
  int r = 0;
  kdata  sd;
  kdata  rd;
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = 0;
  sd.head.type = MTNCMD_READ;
  mtn_set_int(&size,   &sd, sizeof(size));
  mtn_set_int(&offset, &sd, sizeof(offset));
  send_stream(s, &sd);
  while(is_loop){
    recv_stream(s, (uint8_t *)&(rd.head), sizeof(rd.head));
    if(rd.head.size == 0){
      break;
    }else{
      recv_stream(s, rd.data.data, rd.head.size);
      memcpy(buf, rd.data.data, rd.head.size);
      r   += rd.head.size;
      buf += rd.head.size;
    }
  }
  return(r);
}

int mtn_write(int s, char *buf, size_t size, off_t offset)
{
  int r = 0;
  int    sz;
  kdata  sd;
  kdata  rd;
  sz = size;
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.type = MTNCMD_WRITE;
  while(size){
    sd.head.size = 0;
    mtn_set_int(&offset, &sd, sizeof(offset));
    r = mtn_set_data(buf, &sd, size);
    size   -= r;
    buf    += r;
    offset += r;
    send_data_stream(s, &sd);
    recv_data_stream(s, &rd);
    if(rd.head.type == MTNRES_ERROR){
      mtn_set_int(&errno, &rd, sizeof(errno));
      return(-1);
    }
  }
  return(sz);
}

int mtn_close(int s)
{
  lprintf(0, "[debug] %s: CALL s=%d\n", __func__, s);
  if(s == 0){
    return(0);
  }
  close(s);
  lprintf(0, "[debug] %s: EXIT\n", __func__);
  return(0);
}

int mtn_callcmd(ktask *kt)
{
  lprintf(0, "[debug] %s: CALL\n", __func__);
  kt->send.head.ver  = PROTOCOL_VERSION;
  kt->send.head.type = kt->type;
  lprintf(0, "[debug] %s: TYPE=%d\n", __func__, kt->send.head.type);
  if(kt->con){
    kt->res = send_recv_stream(kt->con, &(kt->send), &(kt->recv));
  }else{
    kt->con = mtn_connect(kt->path, kt->create);
    if(kt->con == -1){
      kt->res = -1;
      lprintf(0, "[error] %s: cat't connect %s\n", __func__, kt->path);
    }else{
      kt->res = send_recv_stream(kt->con, &(kt->send), &(kt->recv));
      close(kt->con);
    }
    kt->con = 0;
  }
  if((kt->res == 0) && (kt->recv.head.type == MTNRES_ERROR)){
    kt->res = -1;
    mtn_get_int(&errno, &(kt->recv), sizeof(errno));
    lprintf(0, "[error] %s: %s\n", __func__, strerror(errno));
  }
  lprintf(0, "[debug] %s: EXIT RES=%d\n", __func__, kt->res);
  return(kt->res);
}

//-------------------------------------------------------------------
//
// mtnstatus
//
//-------------------------------------------------------------------
void kcount(int ddir, int dstat, int dmember)
{
  pthread_mutex_lock(&(kopt.status_mutex));
  kopt.cdir    += ddir;
  kopt.cstat   += dstat;
  kopt.cmember += dmember;
  pthread_mutex_unlock(&(kopt.status_mutex));
}

char *get_mtnstatus_members()
{
  int l;
  char *p = NULL;
  pthread_mutex_lock(&(kopt.status_mutex));
  if(l = strlen(kopt.mtnstatus_members.buff)){
    p = malloc(l);
    memcpy(p, kopt.mtnstatus_members.buff, l);
  }
  pthread_mutex_unlock(&(kopt.status_mutex));
  return(p); 
}

char *get_mtnstatus_debuginfo()
{
  int l;
  char *p = NULL;
  pthread_mutex_lock(&(kopt.status_mutex));
  if(l = strlen(kopt.mtnstatus_debuginfo.buff)){
    p = malloc(l);
    memcpy(p, kopt.mtnstatus_debuginfo.buff, l);
  }
  pthread_mutex_unlock(&(kopt.status_mutex));
  return(p); 
}

size_t mtnstatus_members()
{
  char   **buff;
  size_t  *size;
  size_t result;
  kmember *member;
  kmember *members;
  pthread_mutex_lock(&(kopt.status_mutex));
  buff = &(kopt.mtnstatus_members.buff);
  size = &(kopt.mtnstatus_members.size);
  if(*buff){
    **buff = 0;
  }
  members = mtn_info();
  for(member=members;member;member=member->next){
    exsprintf(buff, size, "%s(%s) %llu Bytes Free\n", member->host, mtn_get_v4addr(&(member->addr)), member->free);
  }
  delmembers(members);
  result = strlen(*buff);
  pthread_mutex_unlock(&(kopt.status_mutex));
  return(result);
}

size_t mtnstatus_debuginfo()
{
  kdir    *kd;
  kstat   *ks;
  kmember *km;
  char   **buff;
  size_t  *size;
  size_t result;
  meminfo minfo;

  pthread_mutex_lock(&(kopt.status_mutex));
  buff = &(kopt.mtnstatus_debuginfo.buff);
  size = &(kopt.mtnstatus_debuginfo.size);
  if(*buff){
    **buff = 0;
  }
  get_meminfo(&minfo);
  exsprintf(buff, size, "[DEBUG INFO]\n");
  exsprintf(buff, size, "VSZ   : %llu KB\n", minfo.vsz / 1024);
  exsprintf(buff, size, "RSS   : %llu KB\n", minfo.res / 1024);
  exsprintf(buff, size, "DIR   : %d\n", kopt.cdir);
  exsprintf(buff, size, "STAT  : %d\n", kopt.cstat);
  exsprintf(buff, size, "MEMBER: %d\n", kopt.cmember);
  for(kd=kopt.dircache;kd;kd=kd->next){
    exsprintf(buff, size, "THIS=%p PREV=%p NEXT=%p FLAG=%d PATH=%s\n", kd, kd->prev, kd->next, kd->flag, kd->path);
    for(ks=kd->kst;ks;ks=ks->next){
      exsprintf(buff, size, "  THIS=%p PREV=%p NEXT=%p \t MEMBER=%p HOST=%s NAME=%s\n", ks, ks->prev, ks->next, ks->member, ks->member->host, ks->name);
    }
  }
  result = strlen(*buff);
  pthread_mutex_unlock(&(kopt.status_mutex));
  return(result);
}

