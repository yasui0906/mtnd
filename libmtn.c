/*
 * libmtn.c
 * Copyright (C) 2011 KLab Inc.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include "mtn.h"
#include "libmtn.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>
#include <syslog.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/resource.h>
typedef void (*MTNPROCFUNC)(MTN *mtn, MTNSVR *, MTNDATA *, MTNDATA *, MTNADDR *);

static int is_loop = 1;
static int socket_rcvbuf = 0;
static int count[MTNCOUNT_MAX];
static pthread_mutex_t sqno_mutex  = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t count_mutex = PTHREAD_MUTEX_INITIALIZER;

char *mtncmdstr[]={
  "STARTUP",
  "SHUTDOWN",
  "HELLO",
  "INFO",
  "STAT",
  "LIST",
  "PUT",
  "GET",
  "DEL",
  "DATA",
  "OPEN",
  "READ",
  "WRITE",
  "CLOSE",
  "TRUNCATE",
  "MKDIR",
  "RMDIR",
  "UNLINK",
  "RENAME",
  "CHMOD",
  "CHOWN",
  "GETATTR",
  "SETATTR",
  "SYMLINK",
  "READLINK",
  "UTIME",
  "RESULT",
  "INIT",
  "EXIT",
  "EXEC",
  "STDIN",
  "STDOUT",
  "STDERR",
};

static void clrcount(int id)
{
  pthread_mutex_lock(&(count_mutex));
  count[id] = 0;
  pthread_mutex_unlock(&(count_mutex));
}

static void inccount(int id)
{
  pthread_mutex_lock(&(count_mutex));
  count[id]++;
  pthread_mutex_unlock(&(count_mutex));
}

static void deccount(int id)
{
  pthread_mutex_lock(&(count_mutex));
  count[id]--;
  pthread_mutex_unlock(&(count_mutex));
}

int getcount(int id)
{
  int r;
  pthread_mutex_lock(&(count_mutex));
  r = count[id];
  pthread_mutex_unlock(&(count_mutex));
  return(r);
}

int is_empty(STR str)
{
  if(!str){
    return(1);
  }
  if(*str == 0){
    return(1);
  }
  return(0);
}

int is_numeric(STR str)
{
  while(*str){
    if(*str < '0' || *str > '9'){
      return(0);
    }
    str++;
  }
  return(1);
}

int is_export(MTNSVR *svr)
{
  if(!svr){
    return(0);
  }
  return(svr->flags & MTNMODE_EXPORT);
}

int is_execute(MTNSVR *svr)
{
  if(!svr){
    return(0);
  }
  return(svr->flags & MTNMODE_EXECUTE);
}

int is_grpsvr(MTNSVR *svr, ARG grp)
{
  int i;
  if(!svr){
    return(0);
  }
  if(!grp || !grp[0]){
    return(1);
  }
  if(!svr->groupstr){
    return(grp ? 0 : 1); 
  }
  for(i=0;svr->grouparg[i];i++){
    if(findarg(grp, svr->grouparg[i])){
      return(1);
    }
  }
  return(0);
}

int getpscount()
{
  int i = 0;
  DIR *d = opendir("/proc");
  struct dirent *ent;
  if(!d){
    return(-1);
  }
  while((ent = readdir(d))){
    if(is_numeric(ent->d_name)){
      i++;
    }
  }
  closedir(d);
  return(i);
}

int getprocstat(MTNPROCSTAT *ps)
{
  int i;
  int f;
  int r;
  int t;
  char *ptr  = NULL;
  char *save = NULL;
  char buff[8192];
  char path[PATH_MAX];
  int ctick = sysconf(_SC_CLK_TCK);

  sprintf(path, "/proc/%d/stat", ps->pid);
  f = open(path, O_RDONLY);
  if(f == -1){
    return(-1); 
  }
  r = read(f, buff, sizeof(buff));
  close(f);
  if(r == -1){
    return(-1);
  }
  buff[r] = 0;

  t = 0;
  i = 2;
  strtok_r(buff, " ", &save);
  while((ptr=strtok_r(NULL, " ", &save))){
    switch(i++){
      case 3:
        ps->state = *ptr;
        break;
      case 14:
        ps->utime = atoi(ptr) * 1000 / ctick; // cpu user time [ms]
        break;
      case 15:
        ps->stime = atoi(ptr) * 1000 / ctick; // cpu system time [ms]
        break;
    }
  }
  return(0);
}

int getjobusage(MTNJOB *job)
{
  int i;
  int rtime;
  struct timeval tv;
  struct timeval tr;

  if(!job){
    return(-1);
  }
  gettimeofday(&tv, NULL);
  timersub(&tv, &(job->start), &tr);
  rtime = tr.tv_sec * 1000 + tr.tv_usec / 1000;
  if(!rtime){
    return(-1);
  }

  job->cpu = 0;
  job->ctm = 0;
  job->rtm = rtime;
  for(i=0;i<job->cct;i++){
    job->ctm += job->pstat[i].utime;
    job->ctm += job->pstat[i].stime;
  }
  return(job->cpu = job->ctm * 1000 / rtime);
}

int scanprocess(MTNJOB *job, int job_max)
{
  int i;
  int j;
  pid_t pid;
  pid_t sid;
  pid_t pgid;
  pid_t mysid = getsid(0);
  DIR *d = opendir("/proc");
  struct dirent *ent;

  while((ent = readdir(d))){
    if(!is_numeric(ent->d_name)){
      continue;
    }
    pid = atoi(ent->d_name);
    sid = getsid(pid);
    if(sid != mysid){
      continue;
    }
    pgid = getpgid(pid);
    for(i=0;i<job_max;i++){
      if(!job[i].pid){
        continue;
      }
      if(getpgid(job[i].pid) == pgid){
        for(j=0;j<job[i].cct;j++){
          if(job[i].pstat[j].pid == pid){
            break;
          }
        }
        if(j == job[i].cct){
          (job[i].cct)++;
          job[i].pstat = realloc(job[i].pstat, sizeof(MTNPROCSTAT) * job[i].cct);
          memset(&(job[i].pstat[j]), 0, sizeof(MTNPROCSTAT));
          job[i].pstat[j].pid = pid;
        }
        getprocstat(&(job[i].pstat[j]));
      }
    }
  }
  closedir(d);
  return(0);
}


int getwaittime(MTNJOB *job, int job_max)
{
  int i;
  int w;
  int wtime = 200;
  for(i=0;i<job_max;i++){
    if(!job[i].pid){
      continue;
    }
    if(!job[i].lim){
      continue;
    }
    if(job[i].pstat[0].state == 'T'){
      w = (job[i].ctm * 1000 / job[i].lim) - job[i].rtm;
    }else{
      w  = job[i].ctm * 1000;
      w -= (job[i].rtm + job[i].lim);
      w /= (job[i].lim - 1000);
    }
    if(w <= 0){
      wtime = 100;
    }else if(w < wtime){
      wtime = w;
    }
  }
  return(wtime);
}

static void *xmalloc(size_t size)
{
  void *p = malloc(size);
  if(p){
    inccount(MTNCOUNT_MALLOC);
  }
  return(p);
}

static void *xcalloc(size_t size)
{
  void *p = calloc(1, size);
  if(p){
    inccount(MTNCOUNT_MALLOC);
  }
  return(p);
}

static void *xrealloc(void *p, size_t size)
{
  void *n = realloc(p, size);
  if(n && (p == NULL)){
    inccount(MTNCOUNT_MALLOC);
  }
  return(n);
}

static void xfree(void *p)
{
  if(p){
    deccount(MTNCOUNT_MALLOC);
  }
  free(p);
}

static size_t exsprintf(char **buff, size_t *size, char *fmt, ...)
{
  size_t len;
  char line[4096];
  va_list arg;
  va_start(arg, fmt);
  vsnprintf(line, sizeof(line), fmt, arg);
  va_end(arg);
  len = strlen(line);
  if(*buff){
    len += strlen(*buff);
  }
  while(len >= *size){
    if(*buff){
      *size += 1024;
      *buff  = xrealloc(*buff, *size);
    }else{
      *size  = 1024;
      *buff  = xmalloc(*size);
      **buff = 0;
    }
  }
  strcat(*buff, line);
  return(len);
}

static uint16_t sqno()
{
  uint16_t r;
  static uint16_t sqno = 0;
  pthread_mutex_lock(&sqno_mutex);
  sqno = (sqno == 0) ? getpid() : sqno;
  r = ++sqno;
  pthread_mutex_unlock(&sqno_mutex);
  return(r);
}

static int send_readywait(MTN *mtn, int s)
{
  int e;
  int r;
  struct epoll_event ev;
  struct timeval tv;
  if(mtn->mps_max && (mtn->mps_max < getcount(MTNCOUNT_MPS))){
    gettimeofday(&tv, NULL);
    while(mtn->mpstv.tv_sec == tv.tv_sec){
      usleep(1000000 - tv.tv_usec);
      gettimeofday(&tv, NULL);
    };
  }
  e = epoll_create(1);
  ev.data.fd = s;
  ev.events  = EPOLLOUT;
  if(e == -1){
    return(0);
  }
  if(epoll_ctl(e, EPOLL_CTL_ADD, s, &ev) == -1){
    close(e);
    return(0);
  }
  do{
    r = epoll_wait(e, &ev, 1, 1000);
    if(r == 1){
      close(e);
      return(1);
    }
    if(r == -1){
      if(errno == EINTR){
        continue;
      }
      break;
    }
  }while(is_loop);
  close(e);
  return(0);
}

static int recv_readywait(MTN *mtn, int s)
{
  int r;
  int e;
  struct epoll_event ev;
  ev.data.fd = s;
  ev.events  = EPOLLIN;

  if((e = epoll_create(1)) == -1){
    mtnlogger(mtn, 0, "[error] %s: epoll_create: %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(epoll_ctl(e, EPOLL_CTL_ADD, s, &ev) == -1){
    mtnlogger(mtn, 0, "[error] %s: epoll_ctl: %s\n", __func__, strerror(errno));
    close(e);
    return(-1);
  }
  while(is_loop){
    if((r = epoll_wait(e, &ev, 1, 1000)) == 1){ 
      close(e);
      return(0);
    }
    if(r == -1){
      if(errno == EINTR){
        continue;
      }
      mtnlogger(mtn, 0, "[error] %s: epoll_wait: %s\n", __func__, strerror(errno));
      break;
    }
  }
  close(e);
  return(-1);
}

static int recv_stream(MTN *mtn, int s, void *buff, size_t size)
{
  int r;
	mtnlogger(mtn, 9, "[debug] %s:\n", __func__);
  while(size){
    if(!is_loop){
      return(-1);
    }
    if(recv_readywait(mtn, s) == -1){
      return(-1);
    }
    if(!(r = read(s, buff, size))){
      return(1);
    }
    if(r == -1){
      if(errno == EAGAIN){
        mtnlogger(mtn, 0, "[warn] %s: %s\n", __func__, strerror(errno));
        continue;
      }
      if(errno == EINTR){
        mtnlogger(mtn, 0, "[warn] %s: %s\n", __func__, strerror(errno));
        continue;
      }else{
        mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
        return(-1);
      }
    }
    buff += r;
    size -= r;
  }
  return(0);
}

static int send_stream(MTN *mtn, int s, uint8_t *buff, size_t size)
{
  int r;
	mtnlogger(mtn, 9, "[debug] %s:\n", __func__);
  while(size && send_readywait(mtn, s)){
    r = write(s, buff, size);
    if(r == -1){
      if(errno == EINTR){
        continue;
      }
      return(-1);
    }
    size -= r;
    buff += r;
  }
  return(0);
}

//==================================================
// TCP送受信関数
//==================================================
int send_data_stream(MTN *mtn, int s, MTNDATA *data)
{
	mtnlogger(mtn, 9, "[debug] %s:\n", __func__);
  size_t   size;
  uint8_t *buff;
  size  = sizeof(MTNHEAD);
  size += data->head.size;
  buff  = (uint8_t *)data;
  return(send_stream(mtn, s, buff, size));
}

int recv_data_stream(MTN *mtn, int s, MTNDATA *kd)
{
  int r;
	mtnlogger(mtn, 9, "[debug] %s:\n", __func__);
  if((r = recv_stream(mtn, s, &(kd->head), sizeof(kd->head)))){
    return(r);
  }
  if(kd->head.size > MTN_MAX_DATASIZE){
    mtnlogger(mtn, 0, "[error] %s: data length too long size=%d\n", __func__, kd->head.size);
    return(-1);
  }
  return(recv_stream(mtn, s, &(kd->data), kd->head.size));
}

int send_recv_stream(MTN *mtn, int s, MTNDATA *sd, MTNDATA *rd)
{
  if(!s){
    errno = EBADF;
    return(-1);
  }
  if(send_data_stream(mtn, s, sd) == -1){
    mtnlogger(mtn, 0, "[error] %s: send error %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(recv_data_stream(mtn, s, rd) == -1){
    mtnlogger(mtn, 0, "[error] %s: recv error %s\n", __func__, strerror(errno));
    return(-1);
  }
  return(0);
}

//==================================================
// UDP送受信関数
//==================================================
int send_dgram(MTN *mtn, int s, MTNDATA *data, MTNADDR *addr)
{
  int r;
  int size;
  MTNDATA sd;
  struct timeval tv;
  size = data->head.size + sizeof(MTNHEAD);
  memcpy(&sd, data, size);
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = htons(data->head.size);
  sd.head.sqno = htons(data->head.sqno);
  while(send_readywait(mtn, s)){
    r = sendto(s, &sd, size, 0, &(addr->addr.addr), addr->len);
    if(r == size){
      if(mtn->mps_max){
        gettimeofday(&tv, NULL);
        if(mtn->mpstv.tv_sec != tv.tv_sec){
          mtn->mpstv.tv_sec = tv.tv_sec;
          clrcount(MTNCOUNT_MPS);
        }
        inccount(MTNCOUNT_MPS);
      }
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

int recv_dgram(MTN *mtn, int s, MTNDATA *data, struct sockaddr *addr, socklen_t *alen)
{
  int r = 0;
  while(is_loop){
    r = recvfrom(s, data, sizeof(MTNDATA), 0, addr, alen);
    if(r >= 0){
      break;
    }
    if(errno == EAGAIN){
      return(-1);
    }
    if(errno == EINTR){
      continue;
    }
    mtnlogger(mtn, 0, "[error] %s: %s recv error\n", __func__, strerror(errno));
    return(-1);
  }
  if(r < sizeof(MTNHEAD)){
    mtnlogger(mtn, 0, "[error] %s: head size error\n", __func__);
    return(-1);
  }
  if(data->head.ver != PROTOCOL_VERSION){
    mtnlogger(mtn, 0, "[error] %s: protocol error %d != %d\n", __func__, data->head.ver, PROTOCOL_VERSION);
    return(-1);
  }
  data->head.size = ntohs(data->head.size);
  if(r != data->head.size + sizeof(MTNHEAD)){
    mtnlogger(mtn, 0, "[error] %s: data size error\n", __func__);
    return(-1);
  }  
  data->head.sqno = ntohs(data->head.sqno);
  return(0);
}

//==================================================
// ソケット生成＆初期化
//==================================================
int create_socket(MTN *mtn, int port, int mode)
{
  int s;
  int reuse = 1;
  struct sockaddr_in addr;
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(port);
  addr.sin_addr.s_addr = INADDR_ANY;
  s = socket(AF_INET, mode, 0);
  if(s == -1){
    mtnlogger(mtn, 0, "[error] %s: can't create socket\n", __func__);
    return(-1);
  }
  if(socket_rcvbuf){
    if(setsockopt(s, SOL_SOCKET, SO_RCVBUF, (void *)&(socket_rcvbuf), sizeof(socket_rcvbuf)) == -1){
      mtnlogger(mtn, 0, "[error] %s: %s setsocmtnopt.SO_RCVBUF error\n", __func__, strerror(errno));
    }
  }
  if(setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse)) == -1){
    mtnlogger(mtn, 0, "[error] %s: SO_REUSEADDR error\n", __func__);
    return(-1);
  }
  if(bind(s, (struct sockaddr*)&addr, sizeof(addr)) == -1){
    mtnlogger(mtn, 0, "[error] %s: bind error\n", __func__);
    return(-1);
  }
  return(s);
}

int create_usocket(MTN *mtn)
{
  int s = create_socket(mtn, 0, SOCK_DGRAM);
  if(s == -1){
    return(-1);
  }
  if(fcntl(s, F_SETFL , O_NONBLOCK)){
    mtnlogger(mtn, 0, "[error] %s: O_NONBLOCK %s\n", __func__, strerror(errno));
    return(-1);
  }
  return(s);
}

int create_lsocket(MTN *mtn)
{
  int s = create_socket(mtn, mtn->mcast_port, SOCK_STREAM);
  if(s == -1){
    return(-1);
  }
  if(fcntl(s, F_SETFD, FD_CLOEXEC)){
    close(s);
    mtnlogger(mtn, 0, "[error] %s: FD_CLOEXEC %s\n", __func__, strerror(errno));
    return(-1);
  }
  return(s);
}

int create_msocket(MTN *mtn)
{
  char lpen = 1;
  char mttl = 1;
  struct ip_mreq mg;
  mg.imr_multiaddr.s_addr = inet_addr(mtn->mcast_addr);
  mg.imr_interface.s_addr = INADDR_ANY;

  int s = create_socket(mtn, mtn->mcast_port, SOCK_DGRAM);
  if(s == -1){
    return(-1);
  }
  if(fcntl(s, F_SETFL , O_NONBLOCK)){
    close(s);
    mtnlogger(mtn, 0, "[error] %s: O_NONBLOCK %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(fcntl(s, F_SETFD, FD_CLOEXEC)){
    close(s);
    mtnlogger(mtn, 0, "[error] %s: FD_CLOEXEC %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(setsockopt(s, IPPROTO_IP, IP_ADD_MEMBERSHIP, (void *)&mg, sizeof(mg)) == -1){
    mtnlogger(mtn, 0, "[error] %s: IP_ADD_MEMBERSHIP error\n", __func__);
    close(s);
    return(-1);
  }
  if(setsockopt(s, IPPROTO_IP, IP_MULTICAST_IF,   (void *)&mg.imr_interface.s_addr, sizeof(mg.imr_interface.s_addr)) == -1){
    close(s);
    mtnlogger(mtn, 0, "[error] %s: IP_MULTICAST_IF error\n", __func__);
    return(-1);
  }
  if(setsockopt(s, IPPROTO_IP, IP_MULTICAST_LOOP, (void *)&lpen, sizeof(lpen)) == -1){
    close(s);
    mtnlogger(mtn, 0, "[error] %s: IP_MULTICAST_LOOP error\n", __func__);
    return(-1);
  }
  if(setsockopt(s, IPPROTO_IP, IP_MULTICAST_TTL,  (void *)&mttl, sizeof(mttl)) == -1){
    close(s);
    mtnlogger(mtn, 0, "[error] %s: IP_MULTICAST_TTL error\n", __func__);
    return(-1);
  }
  return(s);
}

//==================================================
// データアクセス関数
//==================================================
char *v4addr(MTNADDR *addr, char *buff)
{
  if(!addr){
    strcpy(buff, "0.0.0.0");
    return(buff);
  }
  if(!inet_ntop(AF_INET, &(addr->addr.in.sin_addr), buff, INET_ADDRSTRLEN)){
    strcpy(buff, "0.0.0.0");
  }
  return(buff);
}

int v4port(MTNADDR *addr)
{
  return(ntohs(addr->addr.in.sin_port));
}

int mtndata_get_string(char *str, MTNDATA *kd)
{
  uint16_t len;
  uint16_t size = kd->head.size;
  uint8_t *buff = kd->data.data;
  if(size > MTN_MAX_DATASIZE){
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

int mtndata_get_svrhost(MTNSVR *svr, MTNDATA *kd)
{
  int  r;
  char buff[1024];
  if((r = mtndata_get_string(buff, kd)) > 0){
    svr->host = modstr(svr->host, buff);
  }
  return(r);
}

static int mtndata_get_int16(uint16_t *val, MTNDATA *kd)
{
  uint16_t len  = sizeof(uint16_t);
  uint16_t size = kd->head.size;
  if(size > MTN_MAX_DATASIZE){
    return(-1);
  }
  if(size < len){
    return(-1);
  }
  size -= len;
  *val = ntohs(kd->data.data16);
  if((kd->head.size = size)){
    memmove(kd->data.data, kd->data.data + len, size);
  }
  return(0);
}

static int mtndata_get_int32(uint32_t *val, MTNDATA *kd)
{
  uint16_t len  = sizeof(uint32_t);
  uint16_t size = kd->head.size;
  if(size > MTN_MAX_DATASIZE){
    return(-1);
  }
  if(size < len){
    return(-1);
  }
  size -= len;
  *val = ntohl(kd->data.data32);
  if((kd->head.size = size)){
    memmove(kd->data.data, kd->data.data + len, size);
  }
  return(0);
}

static int mtndata_get_int64(uint64_t *val, MTNDATA *kd)
{
  uint16_t  len = sizeof(uint64_t);
  uint16_t size = kd->head.size;
  if(size > MTN_MAX_DATASIZE){
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
  if((kd->head.size = size)){
    memmove(kd->data.data, kd->data.data + len, size);
  }
  return(0);
}

int mtndata_get_int(void *val, MTNDATA *kd, int size)
{
  switch(size){
    case 2:
      return mtndata_get_int16(val, kd);
    case 4:
      return mtndata_get_int32(val, kd);
    case 8:
      return mtndata_get_int64(val, kd);
  }
  return(-1);
}

int mtndata_get_data(void *buf, MTNDATA *kd, int size)
{
  if(!buf || !kd){
    return(-1);
  }
  if(kd->head.size > MTN_MAX_DATASIZE){
    return(-1);
  }
  if(kd->head.size < size){
    return(-1);
  }
  memcpy(buf, kd->data.data, size);
  kd->head.size -= size;
  if(kd->head.size){
    memmove(kd->data.data, kd->data.data + size, kd->head.size);
  }
  return(0);
}

int mtndata_set_string(char *str, MTNDATA *kd)
{
  uint16_t len;
  if(str == NULL){
    *(kd->data.data + kd->head.size) = 0;
    kd->head.size++;
    return(0);
  }
  len = strlen(str) + 1;
  if(kd){
    if(kd->head.size + len > MTN_MAX_DATASIZE){
      return(-1);
    }
    memcpy(kd->data.data + kd->head.size, str, len);
    kd->head.size += len;
  }
  return(len);
}

static int mtndata_set_int16(uint16_t *val, MTNDATA *kd)
{
  uint16_t len = sizeof(uint16_t);
  if(kd){
    if(kd->head.size + len > MTN_MAX_DATASIZE){
      return(-1);
    }
    *(uint16_t *)(kd->data.data + kd->head.size) = htons(*val);
    kd->head.size += len;
  }
  return(len);
}

static int mtndata_set_int32(uint32_t *val, MTNDATA *kd)
{
  uint16_t len = sizeof(uint32_t);
  if(kd){
    if(kd->head.size + len > MTN_MAX_DATASIZE){
      return(-1);
    }
    *(uint32_t *)(kd->data.data + kd->head.size) = htonl(*val);
    kd->head.size += len;
  }
  return(len);
}

static int mtndata_set_int64(uint64_t *val, MTNDATA *kd)
{
  uint16_t  len = sizeof(uint64_t);
  uint32_t hval = (*val) >> 32;
  uint32_t lval = (*val) & 0xFFFFFFFF;
  uint32_t *ptr = NULL;
  if(kd){
    ptr = (uint32_t *)(kd->data.data + kd->head.size);
    if(kd->head.size + len > MTN_MAX_DATASIZE){
      return(-1);
    }
    *(ptr + 0) = htonl(hval);
    *(ptr + 1) = htonl(lval);
    kd->head.size += len;
  }
  return(len);
}

int mtndata_set_int(void *val, MTNDATA *kd, int size)
{
  switch(size){
    case 2:
      return mtndata_set_int16(val, kd);
    case 4:
      return mtndata_set_int32(val, kd);
    case 8:
      return mtndata_set_int64(val, kd);
  }
  return(-1);
}

int mtndata_set_data(void *buff, MTNDATA *kd, size_t size)
{
  size_t max = MTN_MAX_DATASIZE - kd->head.size;
  if(MTN_MAX_DATASIZE <= kd->head.size){
    return(0);
  }
  if(size > max){
    size = max;
  }
  memcpy(kd->data.data + kd->head.size, buff, size);
  kd->head.size += size;
  return(size);
}

int mtndata_set_stat(struct stat *st, MTNDATA *kd)
{
  int r = 0;
  int l = 0;
  if(st){
    r = mtndata_set_int(&(st->st_mode),  kd, sizeof(st->st_mode));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtndata_set_int(&(st->st_size),  kd, sizeof(st->st_size));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtndata_set_int(&(st->st_uid),   kd, sizeof(st->st_uid));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtndata_set_int(&(st->st_gid),   kd, sizeof(st->st_gid));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtndata_set_int(&(st->st_blocks),kd, sizeof(st->st_blocks));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtndata_set_int(&(st->st_atime), kd, sizeof(st->st_atime));
    if(r == -1){return(-1);}else{l+=r;}

    r = mtndata_set_int(&(st->st_mtime), kd, sizeof(st->st_mtime));
    if(r == -1){return(-1);}else{l+=r;}
  }
  return(l);
}

int mtndata_get_stat(struct stat *st, MTNDATA *kd)
{
  if(st && kd){
    if(mtndata_get_int(&(st->st_mode),  kd, sizeof(st->st_mode)) == -1){
      return(-1);
    }
    if(mtndata_get_int(&(st->st_size),  kd, sizeof(st->st_size)) == -1){
      return(-1);
    }
    if(mtndata_get_int(&(st->st_uid),   kd, sizeof(st->st_uid)) == -1){
      return(-1);
    }
    if(mtndata_get_int(&(st->st_gid),   kd, sizeof(st->st_gid)) == -1){
      return(-1);
    }
    if(mtndata_get_int(&(st->st_blocks),kd, sizeof(st->st_blocks)) == -1){
      return(-1);
    }
    if(mtndata_get_int(&(st->st_atime), kd, sizeof(st->st_atime)) == -1){
      return(-1);
    }
    if(mtndata_get_int(&(st->st_mtime), kd, sizeof(st->st_mtime)) == -1){
      return(-1);
    }
    st->st_nlink = S_ISDIR(st->st_mode) ? 2 : 1;
    return(0);
  }
  return(-1);
}

char *get_mode_string(mode_t mode)
{
  int m;
  static char mode_string[16];
  char *buff = mode_string;
  char *perm[] = {"---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx"};
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
  return(mode_string);
}

//----------------------------------------------------------------
// MTNDIR
//----------------------------------------------------------------
MTNDIR *newdir(const char *path)
{
  MTNDIR *kd;
  kd = xmalloc(sizeof(MTNDIR));
  memset(kd,0,sizeof(MTNDIR));
  strcpy(kd->path, path);
  inccount(MTNCOUNT_DIR);
  return(kd);
}

MTNDIR *deldir(MTNDIR *kd)
{
	MTNDIR *n;
  if(!kd){
    return(NULL);
  }
  clrstat(kd->st);
  kd->st = NULL;
  if(kd->prev){
    kd->prev->next = kd->next;
  }
  if(kd->next){
    kd->next->prev = kd->prev;
  }
	n = kd->next;
  kd->prev = NULL;
  kd->next = NULL;
  xfree(kd);
  deccount(MTNCOUNT_DIR);
	return(n);
}

//----------------------------------------------------------------
// MTNSTAT
//----------------------------------------------------------------
MTNSTAT *newstat(const char *name)
{
  char b[PATH_MAX];
  MTNSTAT *st = xcalloc(sizeof(MTNSTAT));
  if(!st){
    return(NULL);
  }
  if(!name){
    b[0] = 0;
  }else{
    strcpy(b, name);
  }
  st->name = newstr(basename(b));
  inccount(MTNCOUNT_STAT);
  return(st);
}

MTNSTAT *delstat(MTNSTAT *mst)
{
	MTNSTAT *r = NULL;
  if(!mst){
    return NULL;
  }
  if(mst->prev){
		mst->prev->next = mst->next;
	}
  if(mst->next){
		r = mst->next;
		mst->next->prev = mst->prev;
  }
  mst->prev = NULL;
  mst->next = NULL;
  mst->name = clrstr(mst->name);
  mst->svr  = clrsvr(mst->svr);
  xfree(mst);
  deccount(MTNCOUNT_STAT);
	return(r);
}

MTNSTAT *clrstat(MTNSTAT *mst)
{
	while(mst){
		mst = delstat(mst);
	}
  return(NULL);
}

MTNSTAT *mkstat(MTNSVR *svr, MTNADDR *addr, MTNDATA *data)
{
  MTNSTAT *kst = NULL;
  int len = mtndata_get_string(NULL, data);
  if(len == -1){
    return(NULL);
  }
  if(len == 0){
    return(NULL);
  }
  kst = newstat(NULL);
  kst->name = xrealloc(kst->name, len);
  mtndata_get_string(kst->name,  data);
  mtndata_get_stat(&(kst->stat), data);
  kst->svr = cpsvr(svr);
  if((kst->next = mkstat(svr, addr, data))){
    kst->next->prev = kst;
  }
  return kst;
}

MTNSTAT *mgstat(MTNSTAT *krt, MTNSTAT *kst)
{
  MTNSTAT *st;
  MTNSTAT *rt;
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
          clrsvr(rt->svr);
          rt->svr = st->svr;
          st->svr = NULL;
        }
        if(st == kst){
          kst = (st = delstat(st));
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
    if((rt->next = kst)){
      kst->prev = rt;
    }
  }
  return(krt);
}

MTNSTAT *cpstat(MTNSTAT *mst)
{
  MTNSTAT *ms = NULL;
  MTNSTAT *mr = NULL;
  while(mst){
    ms = newstat(mst->name);
    memcpy(&(ms->stat), &(mst->stat), sizeof(struct stat));
    ms->svr = cpsvr(mst->svr);
    if((ms->next = mr)){
      mr->prev = ms;
    }
    mr = ms;
    mst=mst->next;
  }
  return(mr);
}

//----------------------------------------------------------------
// MTNSVR
//----------------------------------------------------------------
MTNSVR *newsvr()
{
  MTNSVR *svr;
  svr = xmalloc(sizeof(MTNSVR));
  memset(svr, 0, sizeof(MTNSVR));
  inccount(MTNCOUNT_SVR);
  return(svr);
}

MTNSVR *delsvr(MTNSVR *svr)
{
  MTNSVR *nsv;
  if(svr == NULL){
    return(NULL);
  }
  nsv = svr->next;
  if(svr->host){
    clrstr(svr->host);
    svr->host = NULL;
  }
  if(svr->prev){
    svr->prev->next = svr->next;
  }
  if(svr->next){
    svr->next->prev = svr->prev;
  }
  svr->next = NULL;
  svr->prev = NULL;
  svr->groupstr = clrstr(svr->groupstr);
  svr->grouparg = clrarg(svr->grouparg);
  xfree(svr);
  deccount(MTNCOUNT_SVR);
  return(nsv);
}

MTNSVR *clrsvr(MTNSVR *svr)
{
  while(svr){
    svr = delsvr(svr);
  }
  return(NULL);
}

MTNSVR *getsvr(MTNSVR *svr, MTNADDR *addr)
{
  while(svr){
    if(cmpaddr(&(svr->addr), addr) == 0){
      return(svr);
    }
    svr = svr->next;
  }
  return(NULL);
}

MTNSVR *addsvr(MTNSVR *svr, MTNADDR *addr, char *host)
{
  MTNSVR *sv = getsvr(svr, addr);
  if(sv == NULL){
    sv = newsvr();
    memcpy(&(sv->addr), addr, sizeof(MTNADDR));
    if((sv->next = svr)){
      svr->prev = sv;
    }
    svr = sv;
  }
  if(host){
    sv->host = modstr(sv->host, host);
  }
  sv->mark = 0;
  return(svr);
}

MTNSVR *cpsvr(MTNSVR *svr)
{
  MTNSVR *nsv;
  if(svr == NULL){
    return(NULL);
  }
  nsv = addsvr(NULL, &(svr->addr), svr->host);
  nsv->mark      = svr->mark;
  nsv->order     = svr->order;
  nsv->dsize     = svr->dsize;
  nsv->dfree     = svr->dfree;
  nsv->vsz       = svr->vsz;
  nsv->res       = svr->res;
  nsv->loadavg   = svr->loadavg;
  nsv->memsize   = svr->memsize;
  nsv->memfree   = svr->memfree;
  nsv->flags     = svr->flags;
  nsv->groupstr  = newstr(svr->groupstr);
  nsv->grouparg  = splitstr(svr->groupstr, ",");
  memcpy(&(nsv->cnt), &(svr->cnt), sizeof(nsv->cnt));
  memcpy(&(nsv->tv),  &(svr->tv),  sizeof(struct timeval));
  return(nsv);
}

MTNSVR *pushsvr(MTNSVR *list, MTNSVR *svr)
{
  if((svr = cpsvr(svr))){
    if((svr->next = list)){
      list->prev = svr;
    }
    list = svr;
  }
  return(list);
}

int cmpsvr(MTNSVR *s1, MTNSVR *s2)
{
  return(cmpaddr(&(s1->addr), &(s2->addr)));
}

static MTNSVR *filtersvr_loadavg(MTNSVR *svr)
{
  MTNSVR *s;
  MTNSVR *r = NULL;
  for(s=svr;s;s=s->next){
    if(s->loadavg < 100){
      r = pushsvr(r, s);
    }
  }
  clrsvr(svr);
  return(r);
}

static MTNSVR *filtersvr_cnt_job(MTNSVR *svr)
{
  int f = 1;
  int j = 0;
  MTNSVR *s = NULL;
  MTNSVR *r = NULL;
  if(!svr){
    return(NULL);
  }
  while(!r && f){
    f = 0;
    for(s=svr;s;s=s->next){
      if(s->cnt.cpu > s->cnt.cld){
        f = 1;
        if(s->cnt.cld == j){
          r = pushsvr(r, s);
        }
      }
    }
    j++;
  }
  if(r){
    clrsvr(svr);
  }else{
    r = svr;
  }
  return(r);
}

static int filtersvr_list_limit(int *list, int count, int level)
{
  int i;
  int sum = 0;
  int sub = 0;
  int avg = 0;
  int lim = 0;
  if(!list || (count < 2)){
    return(0);
  }
  for(i=1;i<count;i++){
    sum += abs(list[i] - list[i-1]);
  }
  lim = list[0];
  avg = sum / (count - 1);
  for(i=1;i<count;i++){
    sub = abs(list[i] - list[i-1]);
    if(avg > sub){
      lim = list[i];
    }else{
      if(level){
        level--;
        lim = list[i];
      }else{
        break;
      }
    }
  }
  return((i == count) ? 0 : lim);
}

static int filtersvr_qsort_cmp1(const void *p1, const void *p2)
{
  int v1 = *(int *)p1;
  int v2 = *(int *)p2;
  if(v1 == v2){
    return(0);
  }
  return((v1 > v2) ? 1 : -1);
}

static int filtersvr_qsort_cmp2(const void *p1, const void *p2)
{
  int v1 = *(int *)p1;
  int v2 = *(int *)p2;
  if(v1 == v2){
    return(0);
  }
  return((v1 < v2) ? 1 : -1);
}

static MTNSVR *filtersvr_cnt_prc(MTNSVR *svr)
{
  int level = 0;
  int count = 0;
  int limit = 0;
  int *list = NULL;
  MTNSVR *s = NULL;
  MTNSVR *r = NULL;

  if(!svr){
    return(NULL);
  }
  for(s=svr;s;s=s->next){
    list = realloc(list, (count + 1) * sizeof(int));
    list[count] = s->cnt.prc;
    count++;
  }
  qsort(list, count, sizeof(int), filtersvr_qsort_cmp1);
  do{
    r = clrsvr(r);
    limit = filtersvr_list_limit(list, count, level++);
    for(s=svr;s;s=s->next){
      if(!limit || (s->cnt.prc <= limit)){
        if(s->cnt.cld * 100 / s->cnt.cpu < 50){
          r = pushsvr(r, s);
        }
      }
    }
    r = filtersvr_cnt_job(r);
  }while(limit && !r);
  free(list);
  clrsvr(svr);
  return(r);
}

static MTNSVR *filtersvr_cnt_cpu(MTNSVR *svr)
{
  int cpu = 0;
  int min = 0;
  MTNSVR *s = NULL;
  MTNSVR *r = NULL;
  if(!svr){
    return(NULL);
  }
  for(s=svr;s;s=s->next){
    cpu = s->cnt.prc * 100 / s->cnt.cpu;
    min = (!min || cpu < min) ? cpu : min;
  }
  for(s=svr;s;s=s->next){
    cpu = s->cnt.prc * 100 / s->cnt.cpu;
    if(cpu == min){
      r = pushsvr(r, s);
    } 
  }
  clrsvr(svr);
  return(r);
}

static MTNSVR *filtersvr_memfree(MTNSVR *svr)
{
  int count = 0;
  int limit = 0;
  int mfree = 0;
  int *list = NULL;
  MTNSVR *s = NULL;
  MTNSVR *r = NULL;

  if(!svr){
    return(NULL);
  }
  for(s=svr;s;s=s->next){
    list = realloc(list, (count + 1) * sizeof(int));
    list[count] = (int)(s->memfree / 1024 / 1024);
    count++;
  }
  qsort(list, count, sizeof(int), filtersvr_qsort_cmp2);
  limit = filtersvr_list_limit(list, count, 0);
  for(s=svr;s;s=s->next){
    mfree = (int)(s->memfree / 1024 / 1024);
    if(!limit || (mfree >= limit)){
      r = pushsvr(r, s);
    }
  }
  free(list);
  clrsvr(svr);
  return(r);
}

MTNSVR *filtersvr_diskfree(MTNSVR *svr)
{
  int count = 0;
  int limit = 0;
  int dfree = 0;
  int *list = NULL;
  MTNSVR *s = NULL;
  MTNSVR *r = NULL;

  if(!svr){
    return(NULL);
  }
  for(s=svr;s;s=s->next){
    list = realloc(list, (count + 1) * sizeof(int));
    list[count] = (int)(s->dfree / 1024 / 1024);
    count++;
  }
  qsort(list, count, sizeof(int), filtersvr_qsort_cmp2);
  limit = filtersvr_list_limit(list, count, 0);
  for(s=svr;s;s=s->next){
    dfree = (int)(s->dfree / 1024 / 1024);
    if(!limit || (dfree >= limit)){
      r = pushsvr(r, s);
    }
  }
  free(list);
  clrsvr(svr);
  return(r);
}

static MTNSVR *filtersvr_order(MTNSVR *svr)
{
  int min;
  MTNSVR *s;
  MTNSVR *r;
  if(!svr){
    return(NULL);
  }
  r = svr;
  min = svr->order;
  for(s=svr->next;s;s=s->next){
    if(min > s->order){
      r = s; 
      min = s->order;
    }
  }
  r = cpsvr(r);
  clrsvr(svr);
  return(r);
}

MTNSVR *filtersvr_export(MTNSVR *svr, uint64_t use)
{
  MTNSVR *s = NULL;
  MTNSVR *r = NULL;
  if(!svr){
    return(NULL);
  }
  for(s=svr;s;s=s->next){
    if(is_export(s)){
      if(use > s->dfree){
        continue;
      }
      r = pushsvr(r, s);
    }
  }
  clrsvr(svr);
  return(r);
}

MTNSVR *filtersvr_execute(MTNSVR *svr)
{
  MTNSVR *s = NULL;
  MTNSVR *r = NULL;
  if(!svr){
    return(NULL);
  }
  for(s=svr;s;s=s->next){
    if(is_execute(s)){
      r = pushsvr(r, s);
    }
  }
  clrsvr(svr);
  return(r);
}


MTNSVR *filtersvr(MTNSVR *s, int mode)
{
  switch(mode){
    case 0:
      s = filtersvr_loadavg(s);  // LAが1以上のノードを除外する
      s = filtersvr_cnt_prc(s);  // プロセス数が少ないノードを抽出する
      s = filtersvr_cnt_cpu(s);  // ジョブが少ないノードを抽出する
      s = filtersvr_memfree(s);  // 空きメモリが多いノードを抽出する
      s = filtersvr_order(s);    // 応答速度が一番速いノードを選択する
      break;
    case 1:
      s = filtersvr_cnt_job(s);  // 忙しいノードを除外
      s = filtersvr_diskfree(s); // ディスクの空き容量が多いノードを抽出
      s = filtersvr_order(s);    // 応答速度が一番速いノードを選択する
      break;
    case 2:
      s = filtersvr_cnt_job(s);  // 忙しいノードを除外
      s = filtersvr_order(s);    // 応答速度が一番速いノードを選択する
      break;
  }
  return(s);
}

//----------------------------------------------------------------
// MTNSAVETASK
//----------------------------------------------------------------
MTNSAVETASK *newsavetask(MTNTASK *t)
{
  MTNSAVETASK *st;
  if((st = xcalloc(sizeof(MTNSAVETASK)))){
    st->sqno = t->recv.head.sqno;
    memcpy(&(st->addr), &(t->addr), sizeof(st->addr));
    memcpy(&(st->tv),   &(t->tv),   sizeof(st->tv));
    inccount(MTNCOUNT_SAVE);
  }
  return(st);
}

MTNSAVETASK *cutsavetask(MTNSAVETASK *t)
{
  MTNSAVETASK *p = NULL;
  MTNSAVETASK *n = NULL;
  if(!t){
    return(NULL);
  }
  p = t->prev;
  n = t->next;
  if(p){
    p->next = n;
  }
  if(n){
    n->prev = p;
  }
  t->prev = NULL;
  t->next = NULL;
  return(n);
}

MTNSAVETASK *delsavetask(MTNSAVETASK *t)
{
  MTNSAVETASK *n;
  if(!t){
    return(NULL);
  }
  n = cutsavetask(t);
  xfree(t);
  deccount(MTNCOUNT_SAVE);
  return(n);
}

//----------------------------------------------------------------
// MTNTASK
//----------------------------------------------------------------
MTNTASK *newtask()
{
  MTNTASK *kt;
  if((kt = xcalloc(sizeof(MTNTASK)))){
    inccount(MTNCOUNT_TASK);
  }
  return(kt);
}

MTNTASK *cuttask(MTNTASK *t)
{
  MTNTASK *p = NULL;
  MTNTASK *n = NULL;
  if(!t){
    return(NULL);
  }
  p = t->prev;
  n = t->next;
  if(p){
    p->next = n;
  }
  if(n){
    n->prev = p;
  }
  t->prev = NULL;
  t->next = NULL;
  return(n);
}

MTNTASK *deltask(MTNTASK *t)
{
  MTNTASK *n;
  if(!t){
    return(NULL);
  }
  n = cuttask(t);
  xfree(t);
  deccount(MTNCOUNT_TASK);
  return(n);
}

//----------------------------------------------------------------
// 
//----------------------------------------------------------------
uint32_t get_task_count(MTNTASK *kt)
{
  uint32_t c = 0;
  while(kt){
    c++;
    kt = kt->next;
  }
  return(c);
}

uint32_t get_members_count(MTNSVR *mb)
{
  uint32_t c = 0;
  while(mb){
    c++;
    mb = mb->next;
  }
  return(c);
}

MTNSVR *get_members(MTN *mtn){
  MTNSVR *mb;
  MTNSVR *members;
  struct timeval tv;
  pthread_mutex_lock(&(mtn->mutex.member));
  gettimeofday(&tv, NULL);
  if((tv.tv_sec - mtn->members.tv.tv_sec) > 15){
    mtn_info_clrcache(mtn);
  }
  if(mtn->members.svr == NULL){
    if((tv.tv_sec - mtn->members.tv.tv_sec) > 5){
      mtn->members.svr = mtn_hello(mtn);
      memcpy(&(mtn->members.tv), &tv, sizeof(struct timeval));
    }
  }
  members = NULL;
  for(mb=mtn->members.svr;mb;mb=mb->next){
    members = pushsvr(members, mb);
  }
  pthread_mutex_unlock(&(mtn->mutex.member));
  return(members);
}

//-------------------------------------------------------------------
// 
//-------------------------------------------------------------------
static int mtn_process_hello(MTN *mtn, int s, MTNDATA *sdata, MTNDATA *rdata, MTNADDR *saddr, MTNADDR *raddr, MTNPROCFUNC mtnproc)
{
  mtnproc(mtn, NULL, sdata, rdata, raddr);
  sdata->head.flag = 1;
  send_dgram(mtn, s, sdata, raddr);
  sdata->head.flag = 0;
  return(sdata->opt32 > get_members_count(sdata->option));
}

static int mtn_process_member(MTN *mtn, int s, MTNSVR *members, MTNDATA *sdata, MTNDATA *rdata, MTNADDR *saddr, MTNADDR *raddr, MTNPROCFUNC mtnproc)
{
  int r = 1;
  MTNSVR *mb;
  members = addsvr(members, raddr, NULL);
  mb = getsvr(members, raddr);
  mtnproc(mtn, mb, sdata, rdata, raddr);
  if(rdata->head.fin){
    mb->mark = 1;
    for(mb=members;mb && mb->mark;mb=mb->next);
    r = (mb == NULL) ? 0 : 1;
  }
  return(r);
}

static int mtn_process_recv(MTN *mtn, int s, MTNSVR *members, MTNDATA *sdata, MTNADDR *saddr, MTNPROCFUNC mtnproc)
{
  int   r = 1;
  MTNADDR raddr;
  MTNDATA rdata;
  memset(&raddr, 0, sizeof(raddr));
  raddr.len = sizeof(raddr.addr);
  while(recv_dgram(mtn, s, &rdata, &(raddr.addr.addr), &(raddr.len)) == 0){
    if(members == NULL){
      r = mtn_process_hello(mtn, s, sdata, &rdata, saddr, &raddr, mtnproc);
    }else{
      r = mtn_process_member(mtn, s, members, sdata, &rdata, saddr, &raddr, mtnproc);
    }
  }
  return(r);
}

static void mtn_process_wait(MTN *mtn, int s, MTNSVR *members, MTNDATA *sdata, MTNADDR *saddr, MTNPROCFUNC mtnfunc)
{
  MTNSVR *mb;
  if(members == NULL){
    send_dgram(mtn, s, sdata, saddr);
  }else{
    for(mb=members;mb;mb=mb->next){
      if(mb->mark == 0){
        send_dgram(mtn, s, sdata, &(mb->addr));
      }
    }
  }
}

static void mtn_process_loop(MTN *mtn, int s, MTNSVR *members, MTNDATA *sdata, MTNADDR *saddr, MTNPROCFUNC mtnproc)
{
  int r;
  int e;
  int l = 1;
  int o = 2000;
  int t = (members == NULL) ? 200 : 3000;
  struct epoll_event ev;
  if(mtnproc == NULL){
    return;
  }
  e = epoll_create(1);
  if(e == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }
  ev.data.fd = s;
  ev.events  = EPOLLIN;
  if(epoll_ctl(e, EPOLL_CTL_ADD, s, &ev) == -1){
    close(e);
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }
  while(is_loop && l){
    r = epoll_wait(e, &ev, 1, t);
    t = 200;
    switch(r){
      case -1:
        if(errno != EINTR){
          mtnlogger(mtn, 0, "[error] %s: epoll %s\n", __func__, strerror(errno));
        }
        break;
      case 0:
        if((l = ((o -= t) > 0))){
          mtn_process_wait(mtn, s, members, sdata, saddr, mtnproc);
        }
        break;
      default:
        l = mtn_process_recv(mtn, s, members, sdata, saddr, mtnproc);
        break;
    }
  }
  close(e);
}

static void mtn_process(MTN *mtn, MTNSVR *members, MTNDATA *sdata, MTNPROCFUNC mtnproc)
{
  int s;
  MTNADDR saddr;
  if((members == NULL) && (mtnproc != NULL ) && (sdata->head.type != MTNCMD_HELLO)){
    return;
  }
  s = create_usocket(mtn);
  if(s == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return;
  }
  saddr.len                     = sizeof(struct sockaddr_in);
  saddr.addr.in.sin_family      = AF_INET;
  saddr.addr.in.sin_port        = htons(mtn->mcast_port);
  saddr.addr.in.sin_addr.s_addr = inet_addr(mtn->mcast_addr);
  sdata->head.ver               = PROTOCOL_VERSION;
  sdata->head.sqno              = sqno();
  if(send_dgram(mtn, s, sdata, &saddr) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
  }else{
    mtn_process_loop(mtn, s, members, sdata, &saddr, mtnproc);
  }
  close(s);
}

//-------------------------------------------------------------------
// 
//-------------------------------------------------------------------
void mtn_startup(MTN *mtn, int f)
{
  MTNDATA data;
  mtnlogger(mtn, 9,"[debug] %s: IN\n", __func__);
  mtnlogger(mtn, 8,"[debug] %s: flag=%d\n", __func__, f);
  data.head.type = MTNCMD_STARTUP;
  data.head.size = 0;
  data.head.flag = f;
  data.option = NULL;
  mtndata_set_string(mtn->host, &data);
  mtn_process(mtn, NULL, &data, NULL);
  mtnlogger(mtn, 9,"[debug] %s: OUT\n", __func__);
}

void mtn_shutdown(MTN *mtn)
{
  MTNDATA data;
  mtnlogger(mtn, 9,"[debug] %s: IN\n", __func__);
  data.head.type = MTNCMD_SHUTDOWN;
  data.head.size = 0;
  data.head.flag = 1;
  data.option = NULL;
  mtndata_set_string(mtn->host, &data);
  mtn_process(mtn, NULL, &data, NULL);
  mtnlogger(mtn, 9,"[debug] %s: OUT\n", __func__);
}

void mtn_hello_process(MTN *mtn, MTNSVR *member, MTNDATA *sdata, MTNDATA *rdata, MTNADDR *addr)
{
  uint16_t order;
  uint32_t mcount;
  char host[1024];
  MTNSVR *members = (MTNSVR *)(sdata->option);
  if(mtndata_get_string(host, rdata) == -1){
    mtnlogger(mtn, 0, "%s: mtn protocol error: hostname\n", __func__);
    return;
  }
  if(mtndata_get_int(&mcount, rdata, sizeof(mcount)) == -1){
    mtnlogger(mtn, 0, "%s: mtn protocol error: mcount\n", __func__);
    return;
  }
  order = members ? members->order + 1 : 0;
  if((members = addsvr(members, addr, host))){
    members->order = order;
  }
  sdata->option = members;
  if(sdata->opt32 < mcount){
    sdata->opt32 = mcount;
  }
}

MTNSVR *mtn_hello(MTN *mtn)
{
  MTNDATA data;
  data.head.type = MTNCMD_HELLO;
  data.head.size = 0;
  data.head.flag = 0;
  data.option = NULL;
  data.opt32  = 0;
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  mtn_process(mtn, NULL, &data, (MTNPROCFUNC)mtn_hello_process);
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
  return((MTNSVR *)(data.option));
}

void mtn_info_process(MTN *mtn, MTNSVR *member, MTNDATA *sdata, MTNDATA *rdata, MTNADDR *addr)
{
  char buff[512];
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  mtndata_get_int(&(member->dsize),     rdata, sizeof(member->dsize));
  mtndata_get_int(&(member->dfree),     rdata, sizeof(member->dfree));
  mtndata_get_int(&(member->vsz),       rdata, sizeof(member->vsz));
  mtndata_get_int(&(member->res),       rdata, sizeof(member->res));
  mtndata_get_int(&(member->cnt.cpu),   rdata, sizeof(member->cnt.cpu));
  mtndata_get_int(&(member->loadavg),   rdata, sizeof(member->loadavg));
  mtndata_get_int(&(member->memsize),   rdata, sizeof(member->memsize));
  mtndata_get_int(&(member->memfree),   rdata, sizeof(member->memfree));
  mtndata_get_int(&(member->flags),     rdata, sizeof(member->flags));
  mtndata_get_data(&(member->cnt),      rdata, sizeof(member->cnt));
  if(mtndata_get_string(buff, rdata) != -1){
    if(strlen(buff)){
      member->groupstr = newstr(buff);
      member->grouparg = splitstr(buff, ",");
    }else{
      member->groupstr = NULL;
      member->grouparg = NULL;
    }
  }
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

void mtn_info_clrcache(MTN *mtn){
  clrsvr(mtn->members.svr);
  mtn->members.svr = NULL;
  memset(&(mtn->members.tv), 0, sizeof(struct timeval));
}

MTNSVR *mtn_info(MTN *mtn)
{
  mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  MTNDATA data;
  MTNSVR *svr;
  MTNSVR *members = NULL;
  MTNSVR *svrlist = get_members(mtn);
  data.head.type  = MTNCMD_INFO;
  data.head.size  = 0;
  data.head.flag  = 0;
  mtn_process(mtn, svrlist, &data, (MTNPROCFUNC)mtn_info_process);
  for(svr=svrlist;svr;svr=svr->next){
    svr->mark = 0;
    if(is_grpsvr(svr, mtn->grouparg)){
      members = pushsvr(members, svr);
    }
  }
  clrsvr(svrlist);
  mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
  return(members);
}

//----------------------------------------------------------------------------
// UDP
//----------------------------------------------------------------------------
void mtn_list_process(MTN *mtn, MTNSVR *member, MTNDATA *sdata, MTNDATA *rdata, MTNADDR *addr)
{
  sdata->option = mgstat(sdata->option, mkstat(member, addr, rdata));
}

MTNSTAT *mtn_list(MTN *mtn, const char *path)
{
	mtnlogger(mtn, 8, "[debug] %s: IN\n", __func__);
	mtnlogger(mtn, 9, "[debug] %s: path=%s\n", __func__, path);
  MTNDATA data;
  MTNSVR *members  = get_members(mtn);
  data.head.type   = MTNCMD_LIST;
  data.head.size   = 0;
  data.head.flag   = 0;
  data.option      = NULL;
  mtndata_set_string((char *)path, &data);
  mtn_process(mtn, members, &data, (MTNPROCFUNC)mtn_list_process);
  clrsvr(members);
	mtnlogger(mtn, 8, "[debug] %s: OUT\n", __func__);
  return(data.option);
}

void mtn_stat_process(MTN *mtn, MTNSVR *member, MTNDATA *sd, MTNDATA *rd, MTNADDR *addr)
{
  MTNSTAT *krt = sd->option;
	if(rd->head.type == MTNCMD_SUCCESS){
		MTNSTAT *kst = mkstat(member, addr, rd);
		sd->option = mgstat(krt, kst);
	}
}

MTNSTAT *mtn_stat(MTN *mtn, const char *path)
{
	mtnlogger(mtn, 8, "[debug] %s:\n", __func__);
  MTNDATA sd;
  MTNSVR *members = get_members(mtn);
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_STAT;
  sd.head.size = 0;
  sd.head.flag = 0;
  sd.option    = NULL;
  mtndata_set_string((char *)path, &sd);
  mtn_process(mtn, members, &sd, (MTNPROCFUNC)mtn_stat_process);
  clrsvr(members);
  return(sd.option);
}

MTNSVR *mtn_choose(MTN *mtn)
{
	mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
  MTNSVR *s = mtn_info(mtn);
	mtnlogger(mtn, 9, "[debug] %s: info=%p\n", __func__, s);
  s = filtersvr_export(s, mtn->choose.use);
	mtnlogger(mtn, 9, "[debug] %s: filtersvr_export=%p\n", __func__, s);
  s = filtersvr(s, mtn->choose.use ? 2 : 1);
	mtnlogger(mtn, 9, "[debug] %s: filtersvr=%p\n", __func__, s);
	mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
  return(s);
}

void mtn_find_process(MTN *mtn, MTNSVR *member, MTNDATA *sdata, MTNDATA *rdata, MTNADDR *addr)
{
	mtnlogger(mtn, 9, "[debug] %s: IN\n", __func__);
	mtnlogger(mtn, 8, "[debug] %s: P=%p  host=%s\n", __func__, sdata->option, member->host);
  sdata->option = mgstat(sdata->option, mkstat(member, addr, rdata));
	mtnlogger(mtn, 8, "[debug] %s: P=%p\n", __func__, sdata->option);
	mtnlogger(mtn, 9, "[debug] %s: OUT\n", __func__);
}

MTNSTAT *mtn_find(MTN *mtn, const char *path, int create_flag)
{
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  MTNSTAT *kst;
  MTNDATA data;
  MTNSVR  *svr;
  MTNSVR  *members = mtn_info(mtn);
  data.option      = NULL;
  data.head.type   = MTNCMD_LIST;
  data.head.size   = 0;
  data.head.flag   = 0;
  mtndata_set_string((char *)path, &data);
  mtn_process(mtn, members, &data, (MTNPROCFUNC)mtn_find_process);
  kst = data.option;
  if(kst == NULL){
    if(create_flag){
      if((svr = mtn_choose(mtn))){
        kst = newstat(path);
        kst->svr = svr;
      }
    }
  }
  clrsvr(members);
  return(kst);
}

void mtn_mkdir_process(MTN *mtn, MTNSVR *member, MTNDATA *sd, MTNDATA *rd, MTNADDR *addr)
{
	if(rd->head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, rd, sizeof(errno));
	  mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
	}
}

int mtn_mkdir(MTN *mtn, const char *path, uid_t uid, gid_t gid)
{
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  MTNDATA sd;
  MTNSVR *members = get_members(mtn);
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_MKDIR;
  sd.head.size = 0;
  sd.head.flag = 0;
  sd.option    = NULL;
  mtndata_set_string((char *)path, &sd);
  mtndata_set_int(&uid, &sd, sizeof(uid));
  mtndata_set_int(&gid, &sd, sizeof(gid));
  mtn_process(mtn, members, &sd, (MTNPROCFUNC)mtn_mkdir_process);
  clrsvr(members);
  return(0);
}

void mtn_rm_process(MTN *mtn, MTNSVR *member, MTNDATA *sd, MTNDATA *rd, MTNADDR *addr)
{
	if(rd->head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, rd, sizeof(errno));
	  mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
	}
}

int mtn_rm(MTN *mtn, const char *path)
{
	mtnlogger(mtn, 2, "[debug] %s: path=%s\n", __func__, path);
  MTNDATA sd;
  MTNSVR *members = get_members(mtn);
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_UNLINK;
  sd.head.size = 0;
  sd.head.flag = 0;
  sd.option    = NULL;
  mtndata_set_string((char *)path, &sd);
  mtn_process(mtn, members, &sd, (MTNPROCFUNC)mtn_rm_process);
  clrsvr(members);
  return(0);
}

void mtn_rename_process(MTN *mtn, MTNSVR *member, MTNDATA *sd, MTNDATA *rd, MTNADDR *addr)
{
	if(rd->head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, rd, sizeof(errno));
	  mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
	}
}

int mtn_rename(MTN *mtn, const char *opath, const char *npath)
{
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  MTNDATA sd;
  MTNSVR *members = get_members(mtn);
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_RENAME;
  sd.head.size = 0;
  sd.head.flag = 0;
  sd.option    = NULL;
  mtndata_set_string((char *)opath, &sd);
  mtndata_set_string((char *)npath, &sd);
  mtn_process(mtn, members, &sd, (MTNPROCFUNC)mtn_rename_process);
  clrsvr(members);
  return(0);
}

void mtn_symlink_process(MTN *mtn, MTNSVR *member, MTNDATA *sd, MTNDATA *rd, MTNADDR *addr)
{
	if(rd->head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, rd, sizeof(errno));
	  mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
	}
}

int mtn_symlink(MTN *mtn, const char *oldpath, const char *newpath)
{
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  MTNDATA sd;
  MTNSVR *members = get_members(mtn);
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_SYMLINK;
  sd.head.size = 0;
  sd.head.flag = 0;
  sd.option    = NULL;
  mtndata_set_string((char *)oldpath, &sd);
  mtndata_set_string((char *)newpath, &sd);
  mtn_process(mtn, members, &sd, (MTNPROCFUNC)mtn_symlink_process);
  clrsvr(members);
  return(0);
}

void mtn_readlink_process(MTN *mtn, MTNSVR *member, MTNDATA *sd, MTNDATA *rd, MTNADDR *addr)
{
  size_t size;
	if(rd->head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, rd, sizeof(errno));
	  mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
	}else{
    if(sd->option == NULL){
      size = mtndata_get_string(NULL, rd);
      sd->option = xmalloc(size);
      mtndata_get_string(sd->option, rd);
    }
  }
}

int mtn_readlink(MTN *mtn, const char *path, char *buff, size_t size)
{
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  MTNDATA sd;
  MTNSVR *members = get_members(mtn);
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_READLINK;
  sd.head.size = 0;
  sd.head.flag = 0;
  sd.option    = NULL;
  mtndata_set_string((char *)path, &sd);
  mtn_process(mtn, members, &sd, (MTNPROCFUNC)mtn_readlink_process);
  clrsvr(members);
  snprintf(buff, size, "%s", (char *)sd.option);
  xfree(sd.option);
  return(0);
}

void mtn_chmod_process(MTN *mtn, MTNSVR *member, MTNDATA *sd, MTNDATA *rd, MTNADDR *addr)
{
	if(rd->head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, rd, sizeof(errno));
	  mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
  }
}

int mtn_chmod(MTN *mtn, const char *path, mode_t mode)
{
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  MTNDATA   sd;
  MTNSVR *m;
  m = get_members(mtn);
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_CHMOD;
  sd.head.size = 0;
  sd.head.flag = 0;
  mtndata_set_string((char *)path, &sd);
  mtndata_set_int(&mode, &sd, sizeof(mode));
  mtn_process(mtn, m, &sd, (MTNPROCFUNC)mtn_chmod_process);
  clrsvr(m);
  return(0);
}

void mtn_chown_process(MTN *mtn, MTNSVR *member, MTNDATA *sd, MTNDATA *rd, MTNADDR *addr)
{
	if(rd->head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, rd, sizeof(errno));
	  mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
  }
}

int mtn_chown(MTN *mtn, const char *path, uid_t uid, gid_t gid)
{
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  MTNDATA   sd;
  MTNSVR *m;
  m = get_members(mtn);
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_CHOWN;
  sd.head.size = 0;
  sd.head.flag = 0;
  mtndata_set_string((char *)path, &sd);
  mtndata_set_int(&uid, &sd, sizeof(uid));
  mtndata_set_int(&gid, &sd, sizeof(gid));
  mtn_process(mtn, m, &sd, (MTNPROCFUNC)mtn_chown_process);
  clrsvr(m);
  return(0);
}

void mtn_utime_process(MTN *mtn, MTNSVR *member, MTNDATA *sd, MTNDATA *rd, MTNADDR *addr)
{
	if(rd->head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, rd, sizeof(errno));
	  mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
  }
}

int mtn_utime(MTN *mtn, const char *path, time_t act, time_t mod)
{
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  MTNDATA   sd;
  MTNSVR *m;
  m = get_members(mtn);
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_UTIME;
  sd.head.size = 0;
  sd.head.flag = 0;
  mtndata_set_string((char *)path, &sd);
  mtndata_set_int(&act, &sd, sizeof(act));
  mtndata_set_int(&mod, &sd, sizeof(mod));
  mtn_process(mtn, m, &sd, (MTNPROCFUNC)mtn_utime_process);
  clrsvr(m);
  return(0);
}

void mtn_truncate_process(MTN *mtn, MTNSVR *member, MTNDATA *sd, MTNDATA *rd, MTNADDR *addr)
{
	if(rd->head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, rd, sizeof(errno));
	  mtnlogger(mtn, 0,"[error] %s: %s %s\n", __func__, member->host, strerror(errno));
  }
}

int mtn_truncate(MTN *mtn, const char *path, off_t offset)
{
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  MTNDATA sd;
  MTNSVR  *m;
  m = get_members(mtn);
	memset(&sd, 0, sizeof(sd));
  sd.head.type = MTNCMD_TRUNCATE;
  sd.head.size = 0;
  sd.head.flag = 0;
  mtndata_set_string((char *)path, &sd);
  mtndata_set_int(&offset, &sd, sizeof(offset));
  mtn_process(mtn, m, &sd, (MTNPROCFUNC)mtn_truncate_process);
  clrsvr(m);
  return(0);
}

//-------------------------------------------------------------------
// TCP
//-------------------------------------------------------------------
static int mtn_connect(MTN *mtn, MTNSVR *svr, MTNINIT *mi)
{
  int s;
  MTNDATA sd;
  MTNDATA rd;

  s = create_socket(mtn, 0, SOCK_STREAM);
  if(s == -1){
    errno = EACCES;
    return(-1);
  }
  if(connect(s, &(svr->addr.addr.addr), svr->addr.len) == -1){
    close(s);
    errno = EACCES;
    return(-1);
  }
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = 0;
  sd.head.flag = 0;
  sd.head.type = MTNCMD_INIT;
  mtndata_set_int(&(mi->uid),  &sd, sizeof(mi->uid));
  mtndata_set_int(&(mi->gid),  &sd, sizeof(mi->gid));
  mtndata_set_int(&(mi->mode), &sd, sizeof(mi->mode));
  mtndata_set_int(&(mi->use),  &sd, sizeof(mi->use));
  if(send_recv_stream(mtn, s, &sd, &rd) == -1){
    close(s);
    return(-1);
  }
  if(rd.head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, &rd, sizeof(errno));
    close(s);
    return(-1);
  }
  return(s);
}

static MTNSVR *mtn_exec_select(MTN *mtn)
{
  MTNSVR *sv;
  sv = mtn_info(mtn);
  return(sv);
}

int mtn_exec_put(MTN *mtn, MTNJOB *job)
{
  int i;
  int f;
  STR path;
  MTNSTAT st;
  if(!job->putarg){
    return(0);
  }
  for(i=0;job->putarg[i];i++){
    if(stat(job->putarg[i], &(st.stat)) == -1){
      mtnlogger(mtn, 0, "[error] %s %s\n", strerror(errno), job->putarg[i]);
      continue;
    }
    f = open(job->putarg[i], O_RDONLY);
    if(f == -1){
      mtnlogger(mtn, 0, "[error] %s %s\n", strerror(errno), job->putarg[i]);
      continue;
    }
    path = newstr(job->putarg[i]);
    mtn_open_file(mtn, job->con, basename(path), O_WRONLY | O_CREAT , &st);
    mtn_put_data(mtn, job->con, f);
    path = clrstr(path);
    close(f);
  }
  return(0);
}

int mtn_exec_get(MTN *mtn, MTNJOB *job)
{
  int i;
  int f;
  MTNSTAT st;
  if(!job->getarg){
    return(0);
  }
  for(i=0;job->getarg[i];i++){
    if(mtn_open_file(mtn, job->con, job->getarg[i], O_RDONLY, &st)){
      mtnlogger(mtn, 0, "[error] %s: open %s %s\n", __func__, strerror(errno), job->getarg[i]);
      continue;
    }
    if(mtn_fgetattr(mtn, job->con, &(st.stat))){
      mtnlogger(mtn, 0, "[error] %s: fgetattr %s %s\n", __func__, strerror(errno), job->getarg[i]);
      continue;
    }
    f = creat(job->getarg[i], st.stat.st_mode);
    if(f == -1){
      mtnlogger(mtn, 0, "[error] %s: creat %s %s\n", __func__, strerror(errno), job->getarg[i]);
      continue;
    }
    mtn_get_data(mtn, job->con, f);
    close(f);
  }
  return(0);
}

int mtn_exec_wait(MTN *mtn, MTNJOB *job)
{
  int r;
  int rsize;
  int ssize;
  char buff[MTN_MAX_DATASIZE];
  struct epoll_event ev;
  MTNDATA sd;
  MTNDATA rd;

  job->efd = epoll_create(1);
  if(job->efd == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }

  ev.data.fd = job->con;
  ev.events  = EPOLLIN;
  if(epoll_ctl(job->efd, EPOLL_CTL_ADD, job->con, &ev) == -1){
    return(-1);
  }

  while(is_loop && !job->fin){
    r = epoll_wait(job->efd, &ev, 1, 1000);
    if(r == 0){
      continue;
    }
    if(r == -1){
      if(errno != EINTR){
        mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
      }
      continue;
    }
    if((r = recv_data_stream(mtn, job->con, &rd)) == 1){
      mtnlogger(mtn, 0, "[error] %s: remote close %s\n", __func__, job->svr->host);
      return(0);
    }
    if(r == -1){
      continue;
    }
    switch(rd.head.type){
      case MTNCMD_ERROR:
      case MTNCMD_SUCCESS:
        job->fin = 1;
        mtn_exec_get(mtn, job);
        mtndata_get_int(&(job->exit), &rd, sizeof(job->exit));
        break;

      case MTNCMD_STDIN:
        sd.head.ver  = PROTOCOL_VERSION;
        sd.head.size = 0;
        sd.head.flag = 0;
        sd.head.type = MTNCMD_STDIN;
        rsize = read(0, buff, sizeof(buff));
        if(rsize > 0){
          mtndata_set_data(buff, &sd, rsize);
        }else if(rsize == -1){
          sprintf(buff, "[error] %s: %s %s\n", __func__, strerror(errno), job->std[0]);
          write(2, buff, strlen(buff));
        }
        send_data_stream(mtn, job->con, &sd);
        break;
      case MTNCMD_STDOUT:
        ssize = 0;
        while(is_loop && (ssize < rd.head.size)){
          r = write(1, (void *)(rd.data.data + ssize), rd.head.size - ssize);
          if(r == -1){
            if((errno == EINTR) || (errno == EAGAIN)){
              continue;
            }
            job->fin  = 1;
            job->exit = 1;
            break;
          }
          ssize += r;
        }
        break;
      case MTNCMD_STDERR:
        ssize = 0;
        while(is_loop && (ssize < rd.head.size)){
          r = write(2, (void *)(rd.data.data + ssize), rd.head.size - ssize);
          if(r == -1){
            if((errno == EINTR) || (errno == EAGAIN)){
              continue;
            }
            job->fin  = 1;
            job->exit = 1;
            break;
          }
          ssize += r;
        }
        break;
    }
  }
  return(0);
}

int mtn_exec(MTN *mtn, MTNJOB *job)
{
  MTNDATA sd;
  MTNDATA rd;
  MTNINIT mi;
  MTNSVR *sv;
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);

  sv = job->svr ? job->svr : mtn_exec_select(mtn);
  if(sv == NULL){
    mtnlogger(mtn, 0, "[error] %s: node not found\n", __func__);
    errno = EACCES;
    return(-1);
  }

  mi.mode  = MTNMODE_EXECUTE;
  mi.uid   = job->uid;
  mi.gid   = job->gid;
  job->con = mtn_connect(mtn, sv, &mi);
  clrsvr(sv);
  if(job->con == -1){
    return(-1);
  }

  if(job->ctl){
    close(job->ctl);
    job->ctl = 0;
  }
  mtn_exec_put(mtn, job);
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = 0;
  sd.head.flag = 0;
  sd.head.type = MTNCMD_EXEC;
  mtndata_set_string(job->cmd, &sd);
  mtndata_set_int(&(job->lim), &sd, sizeof(job->lim));
  if(send_recv_stream(mtn, job->con, &sd, &rd) == -1){
    return(-1);
  }
  if(rd.head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, &rd, sizeof(errno));
    return(-1);
  }
  mtn_exec_wait(mtn, job);
  _exit(job->exit);
}

int mtn_open_file(MTN *mtn, int s, const char *path, int flags, MTNSTAT *st)
{
  MTNDATA sd;
  MTNDATA rd;
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = 0;
  sd.head.flag = 0;
  sd.head.type = MTNCMD_OPEN;
  mtndata_set_string((char *)path, &sd);
  mtndata_set_int(&flags, &sd, sizeof(flags));
  mtndata_set_int(&(st->stat.st_mode), &sd, sizeof(st->stat.st_mode));
  if(send_recv_stream(mtn, s, &sd, &rd) == -1){
    return(-1);
  }
  if(rd.head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, &rd, sizeof(errno));
    return(-1);
  }
  return(0);
}

int mtn_open(MTN *mtn, const char *path, int flags, MTNSTAT *st)
{
  int s;
  MTNINIT  mi;
  MTNSTAT *fs;
  MTNSVR *svr;
  char buff[64];

	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  fs  = NULL;
  svr = cpsvr(st->svr);
  if(svr == NULL){
    fs = mtn_find(mtn, path, ((flags & O_CREAT) != 0));
    if(fs == NULL){
      mtnlogger(mtn, 0, "[error] %s: node not found\n", __func__);
      errno = EACCES;
      return(-1);
    }
    svr = cpsvr(fs->svr);
  }
  mi.uid  = st->stat.st_uid;
  mi.gid  = st->stat.st_gid;
  mi.use  = mtn->choose.use;
  mi.mode = MTNMODE_EXPORT;
  s = mtn_connect(mtn, svr, &mi);
  if(s == -1){
    mtnlogger(mtn, 0, "[error] %s: can't connect: %s %s\n", __func__, strerror(errno), v4addr(&(svr->addr), buff));
    clrstat(fs);
    clrsvr(svr);
    return(-1);
  }
  clrstat(fs);
  clrsvr(svr);
  if(mtn_open_file(mtn, s, path, flags, st) == -1){
    close(s);
    return(-1);
  }
  mtn->sendsize[s] = 0;
  mtn->sendbuff[s] = xrealloc(mtn->sendbuff[s], MTN_TCP_BUFFSIZE);
  return(s);
}

int mtn_read(MTN *mtn, int s, char *buf, size_t size, off_t offset)
{
  int r = 0;
  int    rs;
  MTNDATA  sd;
  MTNDATA  rd;
  while(is_loop && size){
    sd.head.ver  = PROTOCOL_VERSION;
    sd.head.size = 0;
    sd.head.flag = 0;
    sd.head.type = MTNCMD_READ;
    if(mtndata_set_int(&size,   &sd, sizeof(size)) == -1){
      r = -EIO;
      break;
    }
    if(mtndata_set_int(&offset, &sd, sizeof(offset)) == -1){
      r = -EIO;
      break;
    }
    if(send_recv_stream(mtn, s, &sd, &rd) == -1){
      r = -errno;
      break;
    }
    if(rd.head.size == 0){
      break;
    }
    rs = (rd.head.size > size) ? size : rd.head.size;
    memcpy(buf, &(rd.data), rs);
    size   -= rs;
    offset += rs;
    buf    += rs;
    r      += rs;
  }
  return(r);
}

int mtn_flush(MTN *mtn, int s)
{
  MTNDATA sd;
  MTNDATA rd;
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.type = MTNCMD_RESULT;
  sd.head.flag = 0;
  sd.head.size = 0;
  if(mtn->sendsize[s] == 0){
    return(0);
  }
  if(send_stream(mtn, s, mtn->sendbuff[s], mtn->sendsize[s]) == -1){
    mtnlogger(mtn, 0, "[error] %s: send_stream %s\n", __func__, strerror(errno));
    return(-1);
  }
  mtn->sendsize[s] = 0;
  if(send_recv_stream(mtn, s, &sd, &rd) == -1){
    mtnlogger(mtn, 0, "[error] %s: send_recv_stream %s\n", __func__, strerror(errno));
    return(-1);
  }else if(rd.head.type == MTNCMD_ERROR){
    mtndata_get_int(&errno, &rd, sizeof(errno));
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  return(0);
}

int mtn_write(MTN *mtn, int s, const char *buf, size_t size, off_t offset)
{
  int  r = 0;
  int     sz;
  MTNDATA sd;

  sz = size;
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.type = MTNCMD_WRITE;
  sd.head.flag = 1;

  char peer[INET_ADDRSTRLEN];
  socklen_t addr_len;
  struct sockaddr_in addr;
  addr_len = sizeof(addr);
  getpeername(s, (struct sockaddr *)(&addr), &addr_len);
  inet_ntop(AF_INET, &(addr.sin_addr), peer, INET_ADDRSTRLEN);

  while(sz){
    sd.head.size = 0;
    mtndata_set_int(&offset, &sd, sizeof(offset));
    r = mtndata_set_data((void *)buf, &sd, sz);
    sz     -= r;
    buf    += r;
    offset += r;
    if(mtn->sendsize[s] + sd.head.size + sizeof(sd.head) > MTN_TCP_BUFFSIZE){
      if(mtn_flush(mtn, s) == -1){
        r = -errno;
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(-r), peer);
        return(r);
      }
    }
    memcpy(mtn->sendbuff[s] + mtn->sendsize[s], &(sd.head), sizeof(sd.head));
    mtn->sendsize[s] += sizeof(sd.head);
    memcpy(mtn->sendbuff[s] + mtn->sendsize[s], &(sd.data), sd.head.size);
    mtn->sendsize[s] += sd.head.size;
  }
  return(size);
}

int mtn_close_file(MTN *mtn, int s)
{
  int r = 0;
  MTNDATA sd;
  MTNDATA rd;
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  if(s == 0){
    return(0);
  }
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.type = MTNCMD_CLOSE;
  sd.head.size = 0;
  sd.head.flag = 0;
  if(mtn_flush(mtn, s) == -1){
    r = -errno;
  }
  if(send_recv_stream(mtn, s, &sd, &rd) == -1){
    r = -errno;
  }else if(rd.head.type == MTNCMD_ERROR){
    mtndata_set_int(&errno, &rd, sizeof(errno));
    r = -errno;
  }
  return(r);
}

int mtn_close(MTN *mtn, int s)
{
  int r = 0;
  MTNDATA sd;
  MTNDATA rd;
	mtnlogger(mtn, 2, "[debug] %s:\n", __func__);
  if(s == 0){
    return(0);
  }
  r = mtn_close_file(mtn, s);
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.type = MTNCMD_EXIT;
  sd.head.size = 0;
  sd.head.flag = 0;
  if(send_recv_stream(mtn, s, &sd, &rd) == -1){
    r = -errno;
  }else if(rd.head.type == MTNCMD_ERROR){
    mtndata_set_int(&errno, &rd, sizeof(errno));
    r = -errno;
  }
  if(close(s) == -1){
    r = -errno;
  }
  return(r);
}

int mtn_callcmd(MTN *mtn, MTNTASK *kt)
{
  kt->send.head.ver  = PROTOCOL_VERSION;
  kt->send.head.type = kt->type;
  kt->res = send_recv_stream(mtn, kt->con, &(kt->send), &(kt->recv));
  if((kt->res == 0) && (kt->recv.head.type == MTNCMD_ERROR)){
    kt->res = -1;
    mtndata_get_int(&errno, &(kt->recv), sizeof(errno));
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
  }
  return(kt->res);
}

int mtn_fgetattr(MTN *mtn, int s, struct stat *st)
{
  MTNTASK kt;
  memset(&kt, 0, sizeof(kt));
  if(s == 0){
    mtnlogger(mtn, 0, "[error] %s: %s %d\n", __func__, strerror(EBADF), s);
    return(-EBADF);
  }
  kt.type = MTNCMD_GETATTR;
  kt.con  = s;
  if(mtn_callcmd(mtn, &kt) == -1){
  }
  if(mtndata_get_stat(st, &(kt.recv)) == -1){
    mtnlogger(mtn, 0, "[error] %s: mtndata_get_stat %s\n", __func__, strerror(errno));
    return(-EACCES);
  }
  return(0);
}

int mtn_fchown(MTN *mtn, int s, uid_t uid, gid_t gid)
{
  MTNTASK kt;
  memset(&kt, 0, sizeof(kt));
  if(s == 0){
    mtnlogger(mtn, 0, "[error] %s: %s %d\n", __func__, strerror(EBADF), s);
    return(-EBADF);
  }
  kt.type = MTNCMD_CHOWN;
  kt.con  = s;
  mtndata_set_int(&uid, &(kt.send), sizeof(uid));
  mtndata_set_int(&gid, &(kt.send), sizeof(gid));
  return((mtn_callcmd(mtn, &kt) == -1) ? -errno : 0);
}

//-------------------------------------------------------------------
// mtnfile
//-------------------------------------------------------------------
int mtn_get_data(MTN *mtn, int s, int f)
{
  MTNDATA sd;
  MTNDATA rd;
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.size = 0;
  sd.head.flag = 0;
  sd.head.type = MTNCMD_GET;
  send_data_stream(mtn, s, &sd);

  sd.head.size = 0;
  sd.head.type = MTNCMD_SUCCESS;
  while(is_loop){
    if(recv_data_stream(mtn, s, &rd) == -1){
      break;
    }
    if(rd.head.size == 0){
      break;
    }
    write(f, rd.data.data, rd.head.size);
  }
  return(0);
}

int mtn_get(MTN *mtn, int f, char *path)
{
  int i;
  int s;
  MTNSTAT st;
  MTNSVR *sv;
  MTNSVR *sp;
  char buff[PATH_MAX];
  char host[PATH_MAX];
  char file[PATH_MAX];

  if(strlen(path) >= PATH_MAX){
    mtnlogger(mtn, 0, "[error]: path too long %s\n", path);
    return(-1);
  }
  strcpy(buff, path);
  strcpy(file, path);
  memset(host, 0, PATH_MAX);
  for(i=0;buff[i];i++){
    if(buff[i] == ':'){
      buff[i] = 0;
      strcpy(host, buff);
      strcpy(file, buff + i + 1);
      break;
    }
  }
  st.svr = NULL;
  st.stat.st_uid = getuid();
  st.stat.st_gid = getgid();
  if(host[0]){
    sv = mtn_info(mtn);
    for(sp=sv;sp;sp=sp->next){
      if(!strcmp(sp->host, host)){
        st.svr = cpsvr(sp);
        break;
      }
    }
    clrsvr(sv);
    if(!st.svr){
      mtnlogger(mtn, 0, "[error]: host not found %s\n", host);
      return(-1);
    }
  }
  s = mtn_open(mtn, file, O_RDONLY, &st);
  st.svr = clrsvr(st.svr);
  if(s == -1){
    return(-1);
  }
  return(mtn_get_data(mtn, s, f));
}

int mtn_put_data(MTN *mtn, int s, int f)
{
  int r;
  MTNDATA sd;
  MTNDATA rd;

  sd.head.fin  = 0;
  sd.head.ver  = PROTOCOL_VERSION;
  sd.head.type = MTNCMD_PUT;
  while((r = read(f, sd.data.data, sizeof(sd.data.data)))){
    if(r == -1){
      return(-1);
    }
    sd.head.size = r;
    r = send_recv_stream(mtn, s, &sd, &rd);
    if(r == -1){
      return(-1);
    }
    if(rd.head.type == MTNCMD_ERROR){
      mtndata_get_int(&errno, &rd, sizeof(errno));
      return(-1);
    }
  }
  return(0);
}

int mtn_put(MTN *mtn, int f, char *path)
{
  int i;
  int s;
  mode_t u;
  MTNSTAT st;
  MTNSVR *sv;
  MTNSVR *sp;
  char buff[PATH_MAX];
  char host[PATH_MAX];
  char file[PATH_MAX];
  struct timeval tv;

  if(strlen(path) >= PATH_MAX){
    mtnlogger(mtn, 0, "[error]: path too long %s\n", path);
    return(-1);
  }
  if((f == 0) || (fstat(f, &(st.stat)) == -1)){
    u = umask(022);
    umask(u);
    gettimeofday(&tv, NULL);
    st.stat.st_uid   = getuid();
    st.stat.st_gid   = getgid();
    st.stat.st_mode  = 0666 & ~u;
    st.stat.st_atime = tv.tv_sec;
    st.stat.st_mtime = tv.tv_sec;
  }

  strcpy(buff, path); 
  strcpy(file, path); 
  memset(host, 0, PATH_MAX);
  for(i=0;buff[i];i++){
    if(buff[i] == ':'){
      buff[i] = 0;
      strcpy(host, buff);
      strcpy(file, buff + i + 1);
      break;
    }
  }

  st.svr = NULL;
  if(host[0]){
    sv = mtn_info(mtn);
    for(sp=sv;sp;sp=sp->next){
      if(!strcmp(sp->host, host)){
        st.svr = cpsvr(sp);
        break;
      }
    }
    clrsvr(sv);
    if(!st.svr){
      mtnlogger(mtn, 0, "[error]: host not found %s\n", host);
      return(-1);
    }
  }
  s = mtn_open(mtn, file, O_WRONLY | O_CREAT , &st);
  st.svr = clrsvr(st.svr);
  if(s == -1){
    return(-1);
  }
  return(mtn_put_data(mtn, s, f));
}

//-------------------------------------------------------------------
//
// mtnstatus
//
//-------------------------------------------------------------------
char *get_mtnstatus_members(MTN *mtn)
{
  char *p = NULL;
  pthread_mutex_lock(&(mtn->mutex.status));
  if(mtn->status.members.buff){
    p = xmalloc(mtn->status.members.size);
    strcpy(p, mtn->status.members.buff);
  }
  pthread_mutex_unlock(&(mtn->mutex.status));
  return(p); 
}

char *get_mtnstatus_debuginfo(MTN *mtn)
{
  char *p = NULL;
  pthread_mutex_lock(&(mtn->mutex.status));
  if(mtn->status.debuginfo.buff){
    p = xmalloc(mtn->status.debuginfo.size);
    strcpy(p, mtn->status.debuginfo.buff);
  }
  pthread_mutex_unlock(&(mtn->mutex.status));
  return(p); 
}

char *get_mtnstatus_loglevel(MTN *mtn)
{
  char *p = NULL;
  pthread_mutex_lock(&(mtn->mutex.loglevel));
  if(mtn->status.loglevel.buff){
    p = xmalloc(mtn->status.loglevel.size);
    strcpy(p, mtn->status.loglevel.buff);
  }
  pthread_mutex_unlock(&(mtn->mutex.loglevel));
  return(p); 
}

size_t set_mtnstatus_members(MTN *mtn)
{
  char   **buff;
  size_t  *size;
  size_t result;
  uint64_t dsize;
  uint64_t dfree;
  uint64_t pfree;
  uint64_t vsz;
  uint64_t res;
  char ipstr[INET_ADDRSTRLEN];
  MTNSVR *mb;
  MTNSVR *members;
  pthread_mutex_lock(&(mtn->mutex.status));
  buff = &(mtn->status.members.buff);
  size = &(mtn->status.members.size);
  if(*buff){
    **buff = 0;
  }
  members = mtn_info(mtn);
  //exsprintf(buff, size, "%-10s %-15s %s %s %s %s %s\n", "Host", "IP", "Free[%]", "Free[MB]", "Total[MB]", "VSZ", "RSS");
  for(mb=members;mb;mb=mb->next){
    if(!is_export(mb)){
      continue;
    }
    dsize = mb->dsize / 1024 / 1024;
    dfree = mb->dfree / 1024 / 1024;
    pfree = dfree * 100 / dsize;
    vsz   = mb->vsz / 1024;
    res   = mb->res / 1024;
    v4addr(&(mb->addr), ipstr);
    exsprintf(buff, size, "%s %s %d %2d%% %llu %llu %llu %llu %d %d %d.%02d %d\n", 
      mb->host,
      ipstr, 
      mb->cnt.mbr,
      pfree, 
      dfree, 
      dsize, 
      vsz, 
      res, 
      mb->cnt.mem,
      mb->cnt.cpu,
      mb->loadavg / 100,
      mb->loadavg % 100,
      mb->cnt.prc); 
  }
  clrsvr(members);
  result = (*buff == NULL) ? 0 : strlen(*buff);
  pthread_mutex_unlock(&(mtn->mutex.status));
  return(result);
}

size_t set_mtnstatus_debuginfo(MTN *mtn)
{
  statm      sm;
  char   **buff;
  size_t  *size;
  size_t result;

  pthread_mutex_lock(&(mtn->mutex.status));
  buff = &(mtn->status.debuginfo.buff);
  size = &(mtn->status.debuginfo.size);
  if(*buff){
    **buff = 0;
  }
  getstatm(&sm);
  exsprintf(buff, size, "[DEBUG INFO]\n");
  exsprintf(buff, size, "VSZ   : %llu KB\n", sm.vsz / 1024);
  exsprintf(buff, size, "RSS   : %llu KB\n", sm.res / 1024);
  exsprintf(buff, size, "SVR   : %d\n", getcount(MTNCOUNT_SVR));
  exsprintf(buff, size, "DIR   : %d\n", getcount(MTNCOUNT_DIR));
  exsprintf(buff, size, "STAT  : %d\n", getcount(MTNCOUNT_STAT));
  exsprintf(buff, size, "STR   : %d\n", getcount(MTNCOUNT_STR));
  exsprintf(buff, size, "ARG   : %d\n", getcount(MTNCOUNT_ARG));
  exsprintf(buff, size, "MALLOC: %d\n", getcount(MTNCOUNT_MALLOC));
  exsprintf(buff, size, "RCVBUF: %d\n", socket_rcvbuf);
  result = strlen(*buff);
  pthread_mutex_unlock(&(mtn->mutex.status));
  return(result);
}

size_t set_mtnstatus_loglevel(MTN *mtn)
{
  char  **buff;
  size_t *size;
  size_t   len;

  pthread_mutex_lock(&(mtn->mutex.loglevel));
  buff = &(mtn->status.loglevel.buff);
  size = &(mtn->status.loglevel.size);
  if(*buff){
    **buff = 0;
  }
  len = exsprintf(buff, size, "%d\n", mtn->loglevel);
  pthread_mutex_unlock(&(mtn->mutex.loglevel));
  return(len);
}

void free_mtnstatus_members(char *buff)
{
  xfree(buff);
}

void free_mtnstatus_debuginfo(char *buff)
{
  xfree(buff);
}

void free_mtnstatus_loglevel(char *buff)
{
  xfree(buff);
}

void mtndebug(const char *func, char *fmt, ...)
{
  va_list arg;
  struct timeval tv;
  char b[1024];
  va_start(arg, fmt);
  vsnprintf(b, sizeof(b), fmt, arg);
  va_end(arg);
  b[sizeof(b) - 1] = 0;
  gettimeofday(&tv, NULL);
  fprintf(stderr, "%02u.%06u [%05u] [debug] %s: %s", (uint32_t)(tv.tv_sec % 60), (uint32_t)(tv.tv_usec), (uint32_t)(syscall(SYS_gettid)), func, b);
}

void mtndumparg(const char *func, ARG arg)
{
  int i;
  for(i=0;arg[i];i++){
    mtndebug(func, "ARG[%d]: %s\n", i, arg[i]);
  }
  mtndebug(func, "ARG[%d]: NULL\n", i);
}

void mtnlogger(MTN *mtn, int l, char *fmt, ...)
{
  va_list arg;
  struct timeval tv;
  char b[8192];
  if(mtn && (mtn->loglevel < l)){
    return;
  }
  va_start(arg, fmt);
  vsnprintf(b, sizeof(b), fmt, arg);
  va_end(arg);
  b[sizeof(b) - 1] = 0;
  if(mtn ? mtn->logtype : 0){
    gettimeofday(&tv, NULL);
    switch(mtn ? mtn->logmode : MTNLOG_STDERR){
      case MTNLOG_SYSLOG:
        openlog((const char *)(mtn->module_name), 0, LOG_DAEMON);
        syslog(LOG_INFO,"%02u.%06u [%05u] %s", (uint32_t)(tv.tv_sec % 60), (uint32_t)(tv.tv_usec), (uint32_t)(syscall(SYS_gettid)), b);
        break;
      case MTNLOG_STDERR:
        fprintf(stderr, "%02u.%06u [%05u] %s", (uint32_t)(tv.tv_sec % 60), (uint32_t)(tv.tv_usec), (uint32_t)(syscall(SYS_gettid)), b);
        break;
    }
  }else{
    switch(mtn ? mtn->logmode : MTNLOG_STDERR){
      case MTNLOG_SYSLOG:
        openlog(mtn->module_name, 0, LOG_DAEMON);
        syslog(LOG_INFO, "%s", b);
        break;
      case MTNLOG_STDERR:
        fprintf(stderr, "%s", b);
        break;
    }
  }
}

int getstatm(statm *m)
{
  int   i;
  int   f;
  char *r;
  char *s;
  char  buff[256];

  memset(buff, 0, sizeof(buff));
  m->page_size = sysconf(_SC_PAGESIZE);
  f = open("/proc/self/statm", O_RDONLY);
  if(f == -1){
    memset(m, 0, sizeof(statm));
    return(-1);
  }
  read(f, buff, sizeof(buff) - 1);
  close(f);
  i = 1;
  s = NULL;
  r = strtok_r(buff, " ", &s);
  m->vsz = atoi(r) * m->page_size;
  while((r=strtok_r(NULL, " ", &s))){
    switch(++i){
      case 2:
        m->res = atoi(r) * m->page_size;
        break;
      case 3:
        m->share = atoi(r) * m->page_size;
        break;
      case 4:
        m->text = atoi(r) * m->page_size;
        break;
      case 6:
        m->data = atoi(r) * m->page_size;
        break;
    }
  }
  return(0);
}

int getmeminfo(uint64_t *size, uint64_t *free)
{
  FILE *f;
  char key[64];
  char val[64];
  char unit[8];
  char buff[1024];
  uint64_t data;

  *size = 0;
  *free = 0;
  f = fopen("/proc/meminfo","r");
  while(fgets(buff, sizeof(buff), f)){
    key[0]=0;
    val[0]=0;
    unit[0]=0;
    sscanf(buff, "%s%s%s", key, val, unit);
    if(strcmp("MemTotal:", key) == 0){
      data = atoi(val);
      if(strcmp("kB", unit) == 0){
        data *= 1024;
      }
      *size = data;
    }
    if(strcmp("MemFree:", key) == 0){
      data = atoi(val);
      if(strcmp("kB", unit) == 0){
        data *= 1024;
      }
      *free += data;
    }
    if(strcmp("Buffers:", key) == 0){
      data = atoi(val);
      if(strcmp("kB", unit) == 0){
        data *= 1024;
      }
      *free += data;
    }
    if(strcmp("Cached:", key) == 0){
      data = atoi(val);
      if(strcmp("kB", unit) == 0){
        data *= 1024;
      }
      *free += data;
    }
  }
  fclose(f);
  return(0);
}


void mtn_break()
{
  is_loop = 0;
}

static int mtn_init_sockopt(MTN *mtn)
{
  FILE *fp;
  char buff[256];
  if((fp = fopen("/proc/sys/net/core/rmem_max", "r"))){
    if(fread(buff, 1, sizeof(buff), fp) > 0){
      socket_rcvbuf = atoi(buff);
    }
    fclose(fp);
  }
  return(0);
}

static int mtn_init_mutex(MTN *mtn)
{
  pthread_mutex_init(&(mtn->mutex.member),   NULL);
  pthread_mutex_init(&(mtn->mutex.status),   NULL);
  pthread_mutex_init(&(mtn->mutex.loglevel), NULL);
  return(0);
}

static int mtn_init_sendbuff(MTN *mtn)
{
  struct rlimit rlim;
  getrlimit(RLIMIT_NOFILE, &rlim);
  mtn->max_open = (rlim.rlim_cur - 32) / 2;
  size_t ss = mtn->max_open * sizeof(size_t);
  size_t bs = mtn->max_open * sizeof(uint8_t *);
  mtn->sendsize = xmalloc(ss);
  mtn->sendbuff = xmalloc(bs);
  memset(mtn->sendsize, 0, ss);
  memset(mtn->sendbuff, 0, bs);
  return(0);
}

MTN *mtn_init(const char *name)
{
  MTN *mtn = calloc(1, sizeof(MTN));

  if(!mtn){
    mtnlogger(NULL, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(NULL);
  }

  memset(count, 0, sizeof(count));
  mtn->max_packet_size = 1024;
  mtn->mcast_port = MTN_DEFAULT_PORT;
  sprintf(mtn->mcast_addr,  MTN_DEFAULT_ADDR);
  sprintf(mtn->module_name, name ? name : "");
  if(mtn_init_sockopt(mtn) == -1){
    mtnlogger(NULL, 0, "[error] %s: sockopt: %s\n", __func__, strerror(errno));
    free(mtn);
    mtn = NULL;
    return(NULL);
  }
  if(mtn_init_mutex(mtn) == -1){
    mtnlogger(NULL, 0, "[error] %s: mutex: %s\n", __func__, strerror(errno));
    free(mtn);
    mtn = NULL;
    return(NULL);
  }
  if(mtn_init_sendbuff(mtn) == -1){
    mtnlogger(NULL, 0, "[error] %s: sendbuff: %s\n", __func__, strerror(errno));
    free(mtn);
    mtn = NULL;
    return(NULL);
  }
  return(mtn);
}

int get_mtn_loglevel(MTN *mtn)
{
  int d;
  pthread_mutex_lock(&(mtn->mutex.loglevel));
  d = mtn->loglevel;
  pthread_mutex_unlock(&(mtn->mutex.loglevel));
  return(d);
}

void set_mtn_loglevel(MTN *mtn, int d)
{
  pthread_mutex_lock(&(mtn->mutex.loglevel));
  mtn->loglevel = d;
  pthread_mutex_unlock(&(mtn->mutex.loglevel));
}

int cmpaddr(MTNADDR *a1, MTNADDR *a2)
{
  void *p1 = &(a1->addr.in.sin_addr);
  void *p2 = &(a2->addr.in.sin_addr);
  size_t size = sizeof(a1->addr.in.sin_addr);
  if(memcmp(p1, p2, size) != 0){
    return(1);
  }
  if(a1->addr.in.sin_port != a2->addr.in.sin_port){
    return(1);
  }
  return(0);
}

char lastchar(char *str)
{
  int l = strlen(str);
  return((l > 0) ? str[l - 1] : 0);
}

STR newstr(char *str)
{
  char *nstr;
  if(!str){
    return(NULL);
  }
  nstr = xmalloc(strlen(str) + 1);
  strcpy(nstr, str);
  inccount(MTNCOUNT_STR);
  return(nstr);
}

STR modstr(STR str, char *n)
{
  STR nstr;
  if(!n){
    xfree(str);
    return(NULL);
  }
  
  nstr = xrealloc(str, strlen(n) + 1);
  strcpy(nstr, n);
  if(!str && nstr){ 
    inccount(MTNCOUNT_STR);
  }
  return(nstr);
}

STR catstr(STR str1, STR str2)
{
  int len = 1;
  len += str1 ? strlen(str1) : 0;
  len += str2 ? strlen(str2) : 0;
  str1 = str1 ? xrealloc(str1, len) : xcalloc(len);
  if(str2){
    strcat(str1, str2);
  }
  return(str1);
}

STR clrstr(STR str)
{
  if(str){
    xfree(str);
    deccount(MTNCOUNT_STR);
  }
  return(NULL);
}

STR dotstr(STR str)
{
  char *p;
  char buff[PATH_MAX];
  strcpy(buff, str);
  p = buff + strlen(buff);
  while(p > buff){
    if(*p == '.'){
      *p = 0;
      break;
    }
    p--;
  }
  return(modstr(str, buff));
}

STR basestr(STR str)
{
  char buff[PATH_MAX];
  if(is_empty(str)){
    return(str);
  }
  strcpy(buff, str);
  return(modstr(str, basename(buff)));
}

ARG splitstr(STR str, STR delim)
{
  char *save = NULL;
  STR ptr = NULL;
  STR buf = newstr(str);
  ARG arg = newarg(0);
  if(buf){
    ptr = strtok_r(buf, delim, &save);
    while(ptr){
      arg = addarg(arg, ptr);
      ptr = strtok_r(NULL, delim, &save);
    }
    clrstr(buf);
  }
  return(arg);
}

ARG newarg(int c)
{
  inccount(MTNCOUNT_ARG);
  return(calloc(c + 1, sizeof(STR)));
}

ARG addarg(ARG arg, STR str)
{
  int i = 0;
  if(!arg){
    arg = newarg(1);
    arg[0] = newstr(str);
  }else{
    for(i=0;arg[i];i++);
    arg = realloc(arg, sizeof(STR) * (i + 2));
    arg[i + 0] = newstr(str);
    arg[i + 1] = NULL;
  }
  return(arg);
}

ARG clrarg(ARG args)
{
  int i;
  if(!args){
    return(NULL);
  }
  for(i=0;args[i];i++){
    clrstr(args[i]);
    args[i] = NULL;
  }
  free(args);
  deccount(MTNCOUNT_ARG);
  return(NULL);
}

STR joinarg(ARG args, STR delim){
  int i;
  STR str; 
  if(!args){
    return(NULL);
  }
  if((str = newstr(args[0]))){
    for(i=1;args[i];i++){
      str = catstr(str, delim);
      str = catstr(str, args[i]);
    }
  }
  return(str);
}

ARG copyarg(ARG args)
{
  int c;
  ARG n;
  if(!args){
    return(NULL);
  }
  for(c=0;args[c];c++);
  if((n = newarg(c))){
    for(c=0;args[c];c++){
      n[c] = newstr(args[c]);
    }
  }
  return(n);
}

STR poparg(ARG args)
{
  int c;
  STR s;
  if(!args){
    return(NULL);
  }
  for(c=0;args[c];c++);
  if(c == 0){
    return(NULL);
  }
  c--;
  s = args[c];
  args[c] = NULL;
  return(s);
}

STR sftarg(ARG args)
{
  int c;
  STR s;
  if(!args){
    return(NULL);
  }
  for(c=0;args[c];c++);
  s = args[0];
  memmove(args, &args[1], sizeof(STR) * c);
  return(s);
}

STR findarg(ARG arg, STR str)
{
  int i;
  if(!arg || !str){
    return(NULL);
  }
  for(i=0;arg[i];i++){
    if(!strcmp(arg[i], str)){
      return(arg[i]);
    }
  }
  return(NULL);
}

int cntarg(ARG arg)
{
  int i = 0;
  if(arg){
    for(i=0;arg[i];i++);
  }
  return(i);
}

static int job_flush_stdout(MTNJOB *job)
{
  int r;
  size_t size = 0;
  while(job->stdout.datasize - size){
    r = write(1, job->stdout.stdbuff + size, job->stdout.datasize - size);
    if(r == -1){
      if(errno == EINTR){
        continue;
      }
      return(-1);
    }
    size += r;
  }
  if(job->stdout.stdbuff){
    free(job->stdout.stdbuff);
    job->stdout.stdbuff  = NULL;
    job->stdout.buffsize = 0;
    job->stdout.datasize = 0;
  }
  return(0);
}

static int job_flush_stderr(MTNJOB *job)
{
  int r;
  size_t size = 0;
  while(job->stderr.datasize - size){
    r = write(2, job->stderr.stdbuff + size, job->stderr.datasize - size);
    if(r == -1){
      if(errno == EINTR){
        continue;
      }
      return(-1);
    }
    size += r;
  }
  if(job->stderr.stdbuff){
    free(job->stderr.stdbuff);
    job->stderr.stdbuff  = NULL;
    job->stderr.buffsize = 0;
    job->stderr.datasize = 0;
  }
  return(0);
}

int job_close(MTNJOB *job)
{
  if(job->efd > 0){
    close(job->efd);
    job->efd = 0;
  }
  if(job->out){
    close(job->out);
    job->out = 0;
  }
  if(job->err){
    close(job->err);
    job->err = 0;
  }
  if(job->con > 0){
    close(job->con);
    job->con = 0;
  }
  if(job->cct){
    free(job->pstat);
    job->pstat = NULL;
    job->cct   = 0;
  }
  job_flush_stdout(job);
  job_flush_stderr(job);
  clrstr(job->cmd);
  clrstr(job->echo);
  clrarg(job->std);
  clrarg(job->putarg);
  clrarg(job->getarg);
  clrarg(job->args);
  clrarg(job->argl);
  clrarg(job->argc);
  clrsvr(job->svr);
  memset(job, 0, sizeof(MTNJOB));
  return(0);
}

