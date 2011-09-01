/*
 * libmtnfs.c
 * Copyright (C) 2011 KLab Inc.
 */
#include "mtnfs.h"
#include "common.h"
typedef void (*MTNPROCFUNC)(kmember *member, kdata *send, kdata *recv, kaddr *addr);
kmember *make_member(kmember *members, uint8_t *host, kaddr *addr);
kmember *get_member(kmember *members, kaddr *addr, int mkflag);
kmember *mtn_hello();

int is_loop = 1;
koption kopt;

void mtn_init_option()
{
  memset((void *)&kopt, 0, sizeof(kopt));
  strcpy(kopt.mcast_addr, "224.0.0.110");	
  kopt.mcast_port = 6000;
  kopt.host[0]    = 0;
  kopt.debuglevel = 0;
  kopt.max_packet_size = 1024;
  pthread_mutex_init(&(kopt.cache_mutex),  NULL);
  pthread_mutex_init(&(kopt.member_mutex), NULL);
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

void clear_members(kmember *members)
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

kmember *make_member(kmember *members, uint8_t *host, kaddr *addr)
{
  kmember *member = get_member(members, addr, 0);
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

kmember *copy_member(kmember *member)
{
  if(member == NULL){
    return(NULL);
  }
  kmember *newmember = make_member(NULL, member->host, &(member->addr));
  newmember->mark = member->mark;
  newmember->free = member->free;
  return(newmember);
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
  gettimeofday(&tv, NULL);
  pthread_mutex_lock(&(kopt.member_mutex));
  if(tv.tv_sec - 10 > kopt.member_tv.tv_sec){
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
  kaddr addr;
  kdata rdata;
  fd_set fds;
  struct timeval tv;
  kmember *member;

  if((members == NULL) && (sdata->head.type != MTNCMD_HELLO)){
    return;
  }
  int s = create_socket(0, SOCK_DGRAM);
  addr.len                     = sizeof(struct sockaddr_in);
  addr.addr.in.sin_family      = AF_INET;
  addr.addr.in.sin_port        = htons(kopt.mcast_port);
  addr.addr.in.sin_addr.s_addr = inet_addr(kopt.mcast_addr);
  sdata->head.ver              = PROTOCOL_VERSION;
  send_dgram(s, sdata, &addr);

  tv.tv_sec  = 2;
  tv.tv_usec = 0;
  while(is_loop){
    FD_ZERO(&fds);
    FD_SET(s, &fds);
    r = select(1024, &fds, NULL,  NULL, &tv);
    if(members == NULL){
      tv.tv_sec  = 0;
      tv.tv_usec = 100000;
    }else{
      tv.tv_sec  = 2;
      tv.tv_usec = 0;
    }
    if(r == 0){
      break;
    }
    if(r == -1){
      continue; /* error */
    }
    if(!FD_ISSET(s, &fds)){
      continue;
    }
    memset(&addr, 0, sizeof(addr));
    addr.len = sizeof(addr.addr);
    if(recv_dgram(s, &rdata, &(addr.addr.addr), &(addr.len))){
      continue;
    }
    if(!members){
      mtn(NULL, sdata, &rdata, &addr);
    }else{
      member = get_member(members, &addr, 1);
      mtn(member, sdata, &rdata, &addr);
      if(rdata.head.fin){
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
  printf("%s (%llu KBytes Free)\n", member->host, member->free / 1024);
}

void mtn_info()
{
  kdata data;
  kmember *members = get_members();
  data.head.type   = MTNCMD_INFO;
  data.head.size   = 0;
  mtn_process(members, &data, (MTNPROCFUNC)mtn_info_process);
  clear_members(members);
}

kstat *rmstat(kstat *kst)
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
  free(kst);
	return(r);
}

void rmstats(kstat *kst)
{
	while(kst){
		kst = rmstat(kst);
	}
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
  return(kst);
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

kdir *rmkdir(kdir *kd)
{
	kdir *n;
  if(!kd){
    return;
  }
  rmstats(kd->kst);
  kd->kst = NULL;
  if(kd->prev){
    kd->prev->next = kd->next;
  }
  if(kd->next){
    kd->next->prev = kd->prev;
  }
	n = kd->next;
  free(kd);
	return(n);
}

void addstat_dircache(const char *path, kstat *kst)
{
  kdir *kd;
  if(kst == NULL){
    return;
  }
  lprintf(0,"[debug] %s: START PATH=%s\n", __func__, path);
  pthread_mutex_lock(&(kopt.cache_mutex));
  kd = kopt.dircache;
  while(kd){
    if(strcmp(kd->path, path) == 0){
      break;
    }
    kd = kd->next;
  }
  if(kd == NULL){
    kd = malloc(sizeof(kdir));
    memset(kd,0,sizeof(kdir));
    strcpy(kd->path, path);
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
  lprintf(0,"[debug] %s: END\n", __func__, path);
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
    kd = malloc(sizeof(kdir));
    memset(kd,0,sizeof(kdir));
    strcpy(kd->path, path);
    if(kd->next = kopt.dircache){
      kopt.dircache->prev = kd;
    }
    kopt.dircache = kd;
  }
  rmstats(kd->kst);
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
  gettimeofday(&tv, NULL);
  pthread_mutex_lock(&(kopt.cache_mutex));
  kd = kopt.dircache;
  while(kd){
    if(kd->tv.tv_sec < tv.tv_sec - 5){
      rmstats(kd->kst);
      kd->kst  = NULL;
      kd->flag = 0;
    }
    if(kd->tv.tv_sec < tv.tv_sec - 300){
      kd = rmkdir(kd);
      continue;
    }
    if(strcmp(kd->path, path) == 0){
      break;
    }
    kd = kd->next;
  }
  if(kd == NULL){
    kd = malloc(sizeof(kdir));
    memset(kd,0,sizeof(kdir));
    strcpy(kd->path, path);
    kd->kst = NULL;
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
  clear_members(members);
  return(data.option);
}

void mtn_stat_process(kmember *member, kdata *sd, kdata *rd, kaddr *addr)
{
	lprintf(0,"[debug] %s: START\n", __func__);
  kstat *krt = sd->option;
	if(rd->head.type == MTNRES_SUCCESS){
		kstat *kst = mkstat(member, addr, rd);
		sd->option = mgstat(krt, kst);
	}
	lprintf(0,"[debug] %s: END\n", __func__);
}

kstat *mtn_stat(const char *path)
{
	lprintf(0,"[debug] %s: START\n", __func__);
  kdata sd;
  kmember *members = get_members();
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_STAT;
  sd.head.size = 0;
  sd.option    = NULL;
  mtn_set_string((uint8_t *)path, &sd);
  mtn_process(members, &sd, (MTNPROCFUNC)mtn_stat_process);
  clear_members(members);
	lprintf(0,"[debug] %s: END\n", __func__);
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
  clear_members(members);
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
  kmember *members = get_members();
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
  clear_members(members);
  return(kst);
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
  lprintf(0, "[debug] %s: connect %s %s %s (%dM free)\n", __func__, path, st->member->host, inet_ntoa(st->member->addr.addr.in.sin_addr), st->member->free);
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
  sd.head.size = 0;
  sd.head.type = MTNRES_SUCCESS;
  //send_data_stream(s, &sd);
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
  lprintf(0, "[debug] %s: START s=%d\n", __func__, s);
  if(s == 0){
    return(0);
  }
  close(s);
  lprintf(0, "[debug] %s: END\n", __func__);
  return(0);
}

int mtn_callcmd(ktask *kt)
{
  lprintf(0, "[debug] %s: START\n", __func__);
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
  lprintf(0, "[debug] %s: END RES=%d\n", __func__, kt->res);
  return(kt->res);
}

