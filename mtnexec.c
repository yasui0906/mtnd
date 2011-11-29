/*
 * mtnexec.c
 * Copyright (C) 2011 KLab Inc.
 */
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include <mtn.h>
#include <libmtn.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>
#include <getopt.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sched.h>
#include "mtnexec.h"

static MTN *mtn;
static CTX *ctx;
static int is_loop = 1;

void version()
{
  printf("%s version %s\n", MODULE_NAME, MTN_VERSION);
}

void usage()
{
  printf("%s version %s\n", MODULE_NAME, MTN_VERSION);
  printf("usage: %s [MODE] [OPTION] [command [initial-arguments]]\n", MODULE_NAME);
  printf("\n");
  printf("  MODE\n");
  printf("   -R                 # use remote server\n");
  printf("   -A                 # do all servers\n");
  printf("   -RL                # use remote and local server\n");
  printf("   -AL                # do all servers and local server\n");
  printf("   --info             # show mtn infomation\n");
  printf("\n");
  printf("  OPTION\n");
  printf("   -i                 # replace string\n");
  printf("   -j num             # CPU usage limit(0-100[%%])\n");
  printf("   -v                 # verbose\n");
  printf("   -n                 # dryrun\n");
  printf("   -b                 # \n");
  printf("   -B                 # stdout/stderr binary mode\n");
  printf("   -g group           # \n");
  printf("   -P num             # \n");
  printf("   -N num             # \n");
  printf("   -m addr            # mcast addr(default:)\n");
  printf("   -p port            # TCP/UDP port(default: 6000)\n");
  printf("   -h                 # help\n");
  printf("   --version          # version\n");
  printf("   --echo=string      # \n");
  printf("   --stdin=path       # \n");
  printf("   --stdout=path      # \n");
  printf("   --stderr=path      # \n");
  printf("   --put=path,path,,, # \n");
  printf("   --get=path,path,,, # \n");
  printf("\n");
}

STR strnum(STR a, char *n)
{
  int i;
  STR s;
  for(i=0;a[i];i++){
    if(a[i]>='0' && a[i]<='9'){
      n[i] = a[i];
    }else{
      break;
    }
  }
  n[i] = 0;
  s = newstr(a + i);
  clrstr(a);
  return(s);
}

STR convarg4(STR a, STR s)
{
  int i;
  s = newstr(s);
  if(is_empty(a)){
    clrstr(a);
    return(s);
  }
  for(i=0;a[i];i++){
    if(a[i] == '/'){
      s = basestr(s);
      continue;
    }
    if(a[i] == '.'){
      s = dotstr(s);
      continue;
    }
    clrstr(s);
    s = newstr("{");
    s = catstr(s, a);
    s = catstr(s, "}");
    clrstr(a);
    return(s);
  }
  clrstr(a);
  return(s);
}

STR convarg3(STR a, MTNJOB *j)
{
  int m;
  int n;
  STR s;
  char buff[ARG_MAX];

  if(!strcmp(a, "H")){
    if(j->svr){
      a = modstr(a, j->svr->host);
    }else{
      a = modstr(a, "local");
    }
    return(a);
  }

  a = strnum(a, buff);
  if(!strlen(buff)){
    s = joinarg(j->argl, ctx->delim);
  }else{
    n = atoi(buff);
    m = cntarg(j->argl);
    s = newstr((n < m) ? j->argl[n] : "");
  }
  a = convarg4(a, s);
  clrstr(s);
  return(a);
}

STR convarg2(STR a, MTNJOB *j)
{
  int i;
  STR p;
  STR q;
  STR r;
  i = 0;
  p = newstr(a);
  while(*(p + i)){
    if(*(p + i) == '}'){
      *(p + i) = 0;
      i++;
      q = newstr(p);
      r = newstr(p + i);
      q = convarg3(q, j);
      a = modstr(a, q);
      a = catstr(a, r);
      clrstr(q);
      clrstr(r);
      break;
    }
    i++;
  }
  clrstr(p);
  return(a);
}

STR convarg(STR a, MTNJOB *j)
{
  int i;
  STR p;
  STR q;
  if(!a){
    return(NULL);
  }
  if(!j){
    return(a);
  }
  i = 0;
  p = newstr(a);
  while(*(p + i)){
    if(*(p + i) == '{'){
      *(p + i) = 0;
      i++;
      q = newstr(p + i);
      q = convarg2(q, j);
      a = modstr(a, p);
      a = catstr(a, q);
      p = modstr(p, a);
      q = clrstr(q);
      continue;
    }
    i++;
  }
  clrstr(p);
  return(a);
}

ARG cpconvarg(ARG a, MTNJOB *j)
{
  int i;
  ARG r = newarg(0);
  if(!a){
    return(r);
  }
  for(i=0;a[i];i++){
    r = addarg(r, a[i]);
    r[i] = convarg(r[i], j);
  }
  return(r);
}

ARG cmdargs(MTNJOB *job)
{
  int i;
  ARG cmd = newarg(0);
  if(job->args){
    for(i=0;job->args[i];i++){
      if(job->conv){
        cmd = addarg(cmd, convarg(newstr(job->args[i]), job));
      }else{
        cmd = addarg(cmd, newstr(job->args[i]));
      }
    }
  }
  if(!job->conv && job->argl){
    for(i=0;job->argl[i];i++){
      cmd = addarg(cmd, job->argl[i]);
    }
  }
  return(cmd);
}

int getjobcount(int mode)
{
  int i = 0;
  int r = 0;
  for(i=0;i<ctx->job_max;i++){
    if(ctx->job[i].pid){
      switch(mode){
        case 0: // local + remote
          r++;
          break;
        case 1: // local
          r += (ctx->job[i].svr) ? 0 : 1;
          break;
        case 2: // remote 
          r += (ctx->job[i].svr) ? 1 : 0;
          break;
      }
    }
  }
  return(r);
}

MTNSVR *getinfo()
{
  MTNSVR *s;
  MTNSVR *members = NULL;
  MTNSVR *svrlist = NULL;
  svrlist = mtn_info(mtn);
  for(s=svrlist;s;s=s->next){
    if(is_execute(s)){
      members = pushsvr(members, s);
    }
  }
  clrsvr(svrlist);
  return(members);
}

void test()
{
}

void info()
{
  MTNSVR *s;
  uint32_t node  = 0;
  uint64_t msize = 0;
  uint64_t mfree = 0;
  uint64_t dsize = 0;
  uint64_t dfree = 0;
  uint32_t cpu_num = 0;
  for(s=getinfo();s;s=s->next){
    node++;
    cpu_num += s->cnt.cpu;
    msize += s->memsize/1024/1024;
    mfree += s->memfree/1024/1024;
    dsize = (s->dsize * s->fsize - s->limit) / 1024 / 1024;
    dfree = (s->dfree * s->bsize - s->limit) / 1024 / 1024;
    printf("%5s: ",        s->host);
    printf("ORD=%03d ",    s->order);
    printf("CPU=%02d ",    s->cnt.cpu);
    printf("LA=%d.%02d ",  s->loadavg / 100, s->loadavg % 100);
    printf("MS=%luM ",     s->memsize/1024/1024);
    printf("MF=%luM ",     s->memfree/1024/1024);
    printf("DF=%luG(%02lu%%) ", dfree / 1024, dfree * 100 / dsize);
    printf("PS=%d ",       s->cnt.prc);
    printf("VSZ=%luK ",    s->vsz/1024);
    printf("RES=%luK ",    s->res/1024);
    printf("MLC=%d ",      s->cnt.mem);
    printf("TSK=%d ",      s->cnt.tsk);
    printf("TSV=%d ",      s->cnt.tsv);
    printf("SVR=%d ",      s->cnt.svr);
    printf("DIR=%d ",      s->cnt.dir);
    printf("STA=%d ",      s->cnt.sta);
    printf("STR=%d ",      s->cnt.str);
    printf("ARG=%d ",      s->cnt.arg);
    printf("CLD=%d ",      s->cnt.cld);
    printf("\n");
  }
  if(!node){
    printf("node not found");
  }else{
    printf("TOTAL: ");
    printf("%dCPU(%dnode) ", cpu_num, node);
    if(msize){
      printf("MEM=%luM/%luM(%lu%%Free) ",  mfree, msize, mfree * 100 / msize);
    }
  }
  printf("\n");
}

struct option *get_optlist()
{
  static struct option opt[]={
      {"help",    0, NULL, 'h'},
      {"version", 0, NULL, 'V'},
      {"info",    0, NULL, 'F'},
      {"test",    0, NULL, 'T'},
      {"stdin",   1, NULL, 'I'},
      {"stdout",  1, NULL, 'O'},
      {"stderr",  1, NULL, 'E'},
      {"put",     1, NULL, 'S'},
      {"get",     1, NULL, 'G'},
      {"echo",    1, NULL, 'e'},
      {0, 0, 0, 0}
    };
  return(opt);
}

int is_delim(char c)
{
  char *s;
  for(s=ctx->delim;*s;s++){
    if(c == *s){
      return(1);
    }
  }
  return(0);
}

char *cmdline(pid_t pid)
{
  int r;
  int f;
  int s = 0;
  static char buff[ARG_MAX];
  char path[PATH_MAX];
  sprintf(path, "/proc/%d/cmdline", pid);
  memset(buff, 0, sizeof(buff));
  f = open(path, O_RDONLY);
  while((r = read(f, buff + s, sizeof(buff) - s)) > 0);
  close(f);
  return(buff);
}

ARG stdname(MTNJOB *job)
{
  ARG std = newarg(3);
  if(ctx->stdin){
    std[0] = convarg(newstr(ctx->stdin), job);
  }else{
    std[0] = newstr("/dev/null");
  }
  if(ctx->stdout){
    std[1] = convarg(newstr(ctx->stdout), job);
  }else{
    std[1] = newstr("");
  }
  if(ctx->stderr){
    std[2] = convarg(newstr(ctx->stderr), job);
  }else{
    std[2] = newstr("");
  }
  std[3] = NULL;
  return(std);
}

ARG parse(int argc, char *argv[])
{
  int r;
  ARG args;
  if(argc < 2){
    usage();
    exit(0);
  }
  optind = 0;
  while((r = getopt_long(argc, argv, "+hVvnBbig:j:e:I:RALP:N:m:p:d:", get_optlist(), NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'V':
        version();
        exit(0);

      case 'F':
        ctx->info = 1;
        break;

      case 'T':
        test();
        exit(0);

      case 'v':
        ctx->verbose = 1;
        break;

      case 'n':
        ctx->dryrun  = 1;
        break;

      case 'i':
        ctx->conv = 1;
        break;

      case 'g':
        mtn->groupstr = newstr(optarg);
        mtn->grouparg = splitstr(optarg, ",");
        break;

      case 'j':
        ctx->cpu_lim = atoi(optarg) * 10;
        break;

      case 'e':
        ctx->echo = newstr(optarg);
        setbuf(stdout, NULL);
        break;

      case 'I':
        ctx->stdin = newstr(optarg);
        break;

      case 'O':
        ctx->stdout = newstr(optarg);
        break;

      case 'E':
        ctx->stderr = newstr(optarg);
        break;

      case 'S':
        ctx->putarg = splitstr(optarg, ",");
        break;

      case 'G':
        ctx->getarg = splitstr(optarg, ",");
        break;

      case 'B':
        ctx->text = 0;
        break;

      case 'b':
        ctx->nobuf = 0;
        break;

      case 'R':
        ctx->opt_R = 1;
        break;

      case 'A':
        ctx->opt_A = 1;
        break;

      case 'L':
        ctx->opt_L = 1;
        break;

      case 'P':
        ctx->job_max = atoi(optarg);
        break;

      case 'N':
        ctx->arg_num = atoi(optarg);
        break;

      case 'm':
        strcpy(mtn->mcast_addr, optarg);
        break;

      case 'p':
        mtn->mcast_port = atoi(optarg);
        break;

      case 'd':
        ctx->delim = modstr(ctx->delim, optarg);
        break;

      case '?':
        usage();
        exit(1);
    }
  }

  if(ctx->opt_R && ctx->opt_A){
    usage();
    exit(1);
  }
  if(ctx->opt_R){
    ctx->mode = ctx->opt_L ? MTNEXECMODE_HYBRID : MTNEXECMODE_REMOTE;
  }
  if(ctx->opt_A){
    ctx->mode = ctx->opt_L ? MTNEXECMODE_ALL1 : MTNEXECMODE_ALL0;
  }
  if(ctx->info){
    info();
    exit(0);
  }
  ctx->verbose = ctx->dryrun ? 0 : ctx->verbose;

  args = newarg(0);
  while(optind < argc){
    if(strcmp(argv[optind], ":::") == 0){
      optind++;
      ctx->linearg = newarg(0);
      break;
    }
    args = addarg(args, argv[optind++]);
  }

  if(ctx->linearg){
    while(optind < argc){
      ctx->linearg = addarg(ctx->linearg, argv[optind++]);
    }
    ctx->arg_num = ctx->arg_num ? ctx->arg_num : 1;
  }

  if((args[0] == NULL) && (ctx->arg_num == 0)){
    ctx->arg_num = 1;
  }
  return(args);
}

void signal_handler(int n, siginfo_t *info, void *ucs)
{
  char data;
  switch(n){
    case SIGINT:
    case SIGTERM:
      ctx->signal = n;
      is_loop = 0;
      mtn_break();
      break;
    case SIGPIPE:
      break;
    case SIGCHLD:
      data = 0;
      write(ctx->fsig[1], &data, 1);
      break;
  }
}

void set_signal_handler()
{
  struct sigaction sig;
  memset(&sig, 0, sizeof(sig));
  sig.sa_sigaction = signal_handler;
  sig.sa_flags     = SA_SIGINFO;
  if(sigaction(SIGINT,  &sig, NULL) == -1){
    mtnlogger(NULL, 0, "%s: sigaction error SIGINT\n", __func__);
    exit(1);
  }
  if(sigaction(SIGTERM, &sig, NULL) == -1){
    mtnlogger(NULL, 0, "%s: sigaction error SIGTERM\n", __func__);
    exit(1);
  }
  if(sigaction(SIGPIPE, &sig, NULL) == -1){
    mtnlogger(NULL, 0, "%s: sigaction error SIGPIPE\n", __func__);
    exit(1);
  }
  sig.sa_flags = SA_SIGINFO | SA_NOCLDSTOP;
  if(sigaction(SIGCHLD, &sig, NULL) == -1){
    mtnlogger(NULL, 0, "%s: sigaction error SIGCHLD\n", __func__);
    exit(1);
  }
}

char *readline(int f)
{
  int  r;
  int  len = 0;
  char buff[1024];
  while(is_loop){
    r = read(f, buff + len, 1);
    if(r == 0){
      if(len){
        buff[len] = 0;
        break;
      }
      errno = 0;
      return(NULL);
    }
    if(r == -1){
      if(errno == EINTR){
        errno = 0;
        continue;
      }
      return(NULL);
    }
    if(buff[len] == '\n'){
      buff[len] = 0;
      break;
    }
    len++; 
  }
  return(newstr(buff));
}

ARG readargline(int f, ARG arg)
{
  char c;
  int  r;
  int  len = 0;
  char buff[ARG_MAX];

  if(!arg){
    return(NULL);
  }
  while(is_loop){
    if(!(r = read(f, &c, 1))){
      // EOF
      if(len){
        break;
      }
      errno = 0;
      clrarg(arg);
      return(NULL);
    }
    if(r == -1){
      if(errno == EINTR){
        errno = 0;
        continue;
      }
      mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
      clrarg(arg);
      return(NULL);
    }
    if(c == 0 || c == '\n'){
      if(!len){
        continue;
      }
      break;
    }
    if(is_delim(c)){
      if(len){
        buff[len] = 0;
        arg = addarg(arg, buff);
        len = 0;
      }
      continue;
    }
    buff[len++] = c;
  }
  buff[len] = 0;
  return(is_loop ? addarg(arg, buff) : clrarg(arg));
}

ARG readarg(int f, int n)
{
  int i;
  ARG arg = newarg(0);
  for(i=0;i<n;i++){
    if(!(arg = readargline(f, arg))){
      break;
    }
  }
  return(arg);
}

int is_busy()
{
  int i;
  for(i=0;i<ctx->job_max;i++){
    if(ctx->job[i].pid == 0){
      return(0);
    } 
  }
  return(1);
}

int is_running()
{
  int i;
  for(i=0;i<ctx->job_max;i++){
    if(ctx->job[i].pid){
      return(1);
    } 
  }
  return(0);
}

int mtnexec_stdread(int fd, char *buff, int size)
{
  int r;
  do{
    r = read(fd, buff, size);
    if(r != -1){
      break;
    }
    if(errno == EAGAIN){
      return(-1);
    }
    if(errno != EINTR){
      mtnlogger(mtn, 0, "[error] %s: read %s FD=%d buff=%p size=%d\n", __func__, strerror(errno), fd, buff, size);
      return(-1);
    }
  }while(is_loop);
  if(r == 0){
    epoll_ctl(ctx->efd, EPOLL_CTL_DEL, fd, NULL);
  }
  return(r);
}

int mtnexec_stdbuff(int size, char *buff, size_t *datasize, size_t *buffsize, char **databuff)
{
  if(*datasize + size > *buffsize){
    *buffsize += 1024;
    *databuff = realloc(*databuff, *buffsize);
  }
  memcpy(*databuff + *datasize, buff, size);
  *datasize += size;
  return(0);
}

int mtnexec_nobuff_txt(int fd, size_t *datasize, size_t *buffsize, char **databuff)
{
  int i;
  int s;
  int r;
  for(i=0;i < *datasize;i++){
    if(*(*databuff + i) == '\n'){
      i++;
      s = 0;
      while(i > s){
        r = write(fd, *databuff + s, i - s);
        if(r == -1){
          if(errno == EINTR){
            continue;
          }
          if(is_loop){
            mtnlogger(mtn, 0, "[error] %s: %s FD=%d\n", __func__, strerror(errno), fd);
          }
          return(-1);
        }else{
          s += r;
        }
      }
      *datasize -= i;
      memmove(*databuff, *databuff + i, *datasize);
      return(1);
    }
  }
  return(0);
}

int mtnexec_nobuff_bin(int fd, size_t *datasize, size_t *buffsize, char **databuff)
{
  int r;
  while(*datasize){
    r = write(fd, *databuff, *datasize);
    if(r == -1){
      if(errno == EINTR){
        continue;
      }
      mtnlogger(mtn, 0, "[error] %s: %s FD=%d\n", __func__, strerror(errno), fd);
      return(-1);
    }
    if(r == 0){
      return(0);
    }
    if((*datasize -= r)){
      memmove(*databuff, *databuff + r, *datasize);
    }
  }
  return(0);
}

int mtnexec_nobuff(int fd, size_t *datasize, size_t *buffsize, char **databuff)
{
  int r;
  if(ctx->text){
    r = mtnexec_nobuff_txt(fd, datasize, buffsize, databuff);
  }else{
    r = mtnexec_nobuff_bin(fd, datasize, buffsize, databuff);
  }
  return(r);
}

int mtnexec_stdout(MTNJOB *job)
{
  int size;
  char buff[1024];
  if(!job->out){
    return(0);
  }
  size = mtnexec_stdread(job->out, buff, sizeof(buff));
  if(size > 0){
    mtnexec_stdbuff(size, buff, &(job->stdout.datasize), &(job->stdout.buffsize), &(job->stdout.stdbuff));
    while(is_loop && ctx->nobuf && (mtnexec_nobuff(1, &(job->stdout.datasize), &(job->stdout.buffsize), &(job->stdout.stdbuff)) > 0));
  }
  return(size);
}

int mtnexec_stderr(MTNJOB *job)
{
  int size;
  char buff[1024];
  if(!job->err){
    return(0);
  }
  size = mtnexec_stdread(job->err, buff, sizeof(buff));
  if(size > 0){
    mtnexec_stdbuff(size, buff, &(job->stderr.datasize), &(job->stderr.buffsize), &(job->stderr.stdbuff));
    while(is_loop && ctx->nobuf && (mtnexec_nobuff(2, &(job->stderr.datasize), &(job->stderr.buffsize), &(job->stderr.stdbuff)) > 0));
  }
  return(size);
}

int jclose(MTNJOB *job)
{
  if(job->out){
    epoll_ctl(ctx->efd, EPOLL_CTL_DEL, job->out, NULL);
    while(mtnexec_stdout(job) > 0);
  }
  if(job->err){
    epoll_ctl(ctx->efd, EPOLL_CTL_DEL, job->err, NULL);
    while(mtnexec_stderr(job) > 0);
  }
  if(!job->exit && job->echo){
    fprintf(stdout, "%s\n", job->echo);
  }
  return(job_close(job));
}

int scheprocess(MTN *mtn, MTNJOB *job, int job_max, int cpu_lim, int cpu_num)
{
  int i;
  int cpu_id;
  int cpu_use;
  cpu_set_t cpumask;

  cpu_id  = 0;
  cpu_use = 0;
  scanprocess(job, job_max);
  for(i=0;i<job_max;i++){
    if(!job[i].pid){
      continue;
    }
    getjobusage(job + i);
    if(cpu_id != job[i].cid){
      CPU_ZERO(&cpumask);
      CPU_SET(cpu_id, &cpumask);
      if(sched_setaffinity(job[i].pid, cpu_num, &cpumask) == -1){
        mtnlogger(mtn, 0, "[error] %s: sched_setaffinity: %s\n", __func__, strerror(errno));
        job->cid = -1;
      }else{
        job->cid = cpu_id;
      }
    }
    cpu_id  += 1;
    cpu_id  %= cpu_num;
    cpu_use += job[i].cpu;
    //MTNDEBUG("CMD=%s STATE=%c CPU=%d.%d\n", job->cmd, job->pstat[0].state, job->cpu / 10, job->cpu % 10);
  }
  //MTNDEBUG("[CPU=%d.%d%% LIM=%d CPU=%d]\n", ctx->cpu_use / 10, ctx->cpu_use % 10, ctx->cpu_lim / 10, ctx->cnt.cpu);

  if(!cpu_lim){
    return(cpu_use);
  }

  for(i=0;i<job_max;i++){
    if(!job[i].pid){
      continue;
    }
    if(cpu_lim * cpu_num < cpu_use){
      // 過負荷状態
      if(job[i].pstat[0].state != 'T'){
        if(job[i].cpu > cpu_lim){
          kill(-(job[i].pid), SIGSTOP);
          return(cpu_use);
        }
      }
    }else{
      // アイドル状態
      if(job[i].pstat[0].state == 'T'){
        if(job[i].cpu < cpu_lim){
          kill(-(job[i].pid), SIGCONT);
          return(cpu_use);
        }
      }
    }
  }

  for(i=0;i<job_max;i++){
    if(!job[i].pid){
      continue;
    }
    if(job[i].pstat[0].state != 'T'){
      if(job[i].cpu > cpu_lim){
        kill(-(job[i].pid), SIGSTOP);
      }
    }else{
      if(job[i].cpu < cpu_lim){
        kill(-(job[i].pid), SIGCONT);
      }
    }
  }
  return(cpu_use);
}

int mtnexec_poll()
{
  int c;
  int i;
  int r;
  int w;
  pid_t pid;
  int status;
  char data;
  struct epoll_event ev[64];
  struct timeval  polltv;
  struct timeval keikatv;
  static int wtime = 200;

  gettimeofday(&polltv, NULL);
  timersub(&polltv, &(ctx->polltv), &keikatv);
  if(wtime < (keikatv.tv_sec * 1000 + keikatv.tv_usec / 1000)){
    memcpy(&(ctx->polltv), &polltv, sizeof(struct timeval));
    ctx->cpu_use = scheprocess(mtn, ctx->job, ctx->job_max, ctx->cpu_lim, ctx->cpu_num);
    wtime = getwaittime(ctx->job, ctx->job_max);
  }

  c = 0;
  w = wtime;
  if(!(r = epoll_wait(ctx->efd, ev, 64, w))){
    return(c);
  }
  if(r == -1){
    if(errno != EINTR){
      mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    }
    return(c);
  }

  while(r--){
    if(ev[r].data.ptr){
      mtnexec_stdout(ev[r].data.ptr);
      mtnexec_stderr(ev[r].data.ptr);
    }else{
      read(ctx->fsig[0], &data, sizeof(data));
      while((pid = waitpid(-1, &status, WNOHANG)) > 0){
        for(i=0;i<ctx->job_max;i++){
          if(pid == ctx->job[i].pid){
            if(WIFEXITED(status)){
              ctx->job[i].exit = WEXITSTATUS(status);
            }
            jclose(&(ctx->job[i]));
            c++;
            w = 0;
            break;
          }
        }
      }

    }
  }
  return(c);
}

MTNJOB *mtnexec_wait()
{
  int i;
  while(is_busy() && is_loop){
    mtnexec_poll();
  }
  if(is_loop){
    for(i=0;i<ctx->job_max;i++){
      if(ctx->job[i].pid == 0){
        return(&(ctx->job[i]));
      }
    }
  }
  return(NULL);
}

void mtnexec_initjob(MTNJOB *job, ARG arg)
{
  static int id = 0;
  memset(job, 0, sizeof(MTNJOB));
  job->id   = id++;
  job->uid  = getuid();
  job->gid  = getgid();
  job->cid  = -1;
  job->lim  = ctx->cpu_lim;
  job->conv = ctx->conv;
  job->args = copyarg(ctx->cmdargs);
  job->argl = copyarg(arg);
  job->exit = -1;
  job->cid  = -1;
}

MTNJOB *copyjob(MTNJOB *dst, MTNJOB *src)
{
  if(dst){
    memcpy(dst, src, sizeof(MTNJOB));
    dst->std    = copyarg(src->std);
    dst->cmd    = newstr(src->cmd);
    dst->echo   = newstr(src->echo);
    dst->args   = copyarg(src->args);
    dst->argl   = copyarg(src->argl);
    dst->argc   = copyarg(src->argc);
    dst->putarg = copyarg(src->putarg);
    dst->getarg = copyarg(src->getarg);
    dst->svr    = cpsvr(src->svr);
    dst->cct    = 0;
    dst->pstat  = NULL;
  }
  return(dst);
}

void mtnexec_verbose(MTNJOB *job)
{
  if(job->svr){
    mtnlogger(mtn, 0, "[mtnexec] %s: %s ", job->svr->host, job->cmd);
  }else{
    mtnlogger(mtn, 0, "[mtnexec] local: %s ", job->cmd);
  }
  if(strlen(job->std[0]) && strcmp(job->std[0], "/dev/null")){
    mtnlogger(mtn, 0, "stdin=%s ", job->std[0]);
  }
  if(strlen(job->std[1])){
    mtnlogger(mtn, 0, ">%s ", job->std[1]);
  }
  if(strlen(job->std[2])){
    mtnlogger(mtn, 0, "2>%s ", job->std[2]);
  }
  mtnlogger(mtn, 0, "\n");
}

void mtnexec_dryrun(MTNJOB *job)
{
  mtnlogger(mtn, 0, "[dryrun] %s ", job->cmd);
  if(strlen(job->std[0]) && strcmp(job->std[0], "/dev/null")){
    mtnlogger(mtn, 0, "<%s ", job->std[0]);
  }
  if(strlen(job->std[1])){
    mtnlogger(mtn, 0, ">%s ", job->std[1]);
  }
  if(strlen(job->std[2])){
    mtnlogger(mtn, 0, "2>%s ", job->std[2]);
  }
  mtnlogger(mtn, 0, "\n");
}

int mtnexec_fork(MTNSVR *svr, ARG arg)
{
  int f;
  int pp[3][2];
  MTNJOB *job;
  struct epoll_event ev;

  job = mtnexec_wait();
  mtnexec_initjob(job, arg);
  job->svr    = svr;
  job->argc   = cmdargs(job);
  job->cmd    = joinarg(job->argc, " ");
  job->std    = stdname(job);
  job->echo   = (ctx->echo) ? convarg(newstr(ctx->echo), job) : NULL;
  job->putarg = cpconvarg(ctx->putarg, ctx->conv ? job : NULL);
  job->getarg = cpconvarg(ctx->getarg, ctx->conv ? job : NULL);
  if(is_empty(job->cmd)){
    jclose(job);
    return(-1);
  }
  if(ctx->dryrun){
    mtnexec_dryrun(job);
    jclose(job);
    return(0);
  }
  if(ctx->verbose){
    mtnexec_verbose(job);
  }
  pipe(pp[0]);
  pipe(pp[1]);
  pipe(pp[2]);
  gettimeofday(&(job->start), NULL);
  job->pid = fork();
  if(job->pid == -1){
    job->pid = 0;
    close(pp[0][0]);
    close(pp[0][1]);
    close(pp[1][0]);
    close(pp[1][1]);
    close(pp[2][0]);
    close(pp[2][1]);
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(job->pid){
    close(pp[0][1]);
    close(pp[1][1]);
    close(pp[2][1]);
    job->ctl   = pp[0][0];
    job->out   = pp[1][0];
    job->err   = pp[2][0];
    job->cct   = 1;
    job->pstat = calloc(1, sizeof(MTNPROCSTAT));
    job->pstat[0].pid = job->pid;
    fcntl(job->ctl, F_SETFD, FD_CLOEXEC);
    fcntl(job->out, F_SETFD, FD_CLOEXEC);
    fcntl(job->err, F_SETFD, FD_CLOEXEC);
    fcntl(job->out, F_SETFL, O_NONBLOCK);
    fcntl(job->err, F_SETFL, O_NONBLOCK);
    ev.data.ptr = job;
    ev.events   = EPOLLIN;
    if(epoll_ctl(ctx->efd, EPOLL_CTL_ADD, job->out, &ev) == -1){
      mtnlogger(mtn, 0, "[error] %s: epoll_ctl %s stdout fd=%d\n", __func__, strerror(errno), job->out);
    }
    if(epoll_ctl(ctx->efd, EPOLL_CTL_ADD, job->err, &ev) == -1){
      mtnlogger(mtn, 0, "[error] %s: epoll_ctl %s stderr fd=%d\n", __func__, strerror(errno), job->err);
    }
    char d;
    while(read(job->ctl,&d,1));
    close(job->ctl);
    job->ctl = 0;
    return(0);
  }

  //===== execute process =====
  setpgid(0,0);
  close(pp[0][0]);
  close(pp[1][0]);
  close(pp[2][0]);
  if(strlen(job->std[0])){
    f = open(job->std[0], O_RDONLY);
    if(f == -1){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), job->std[0]);
      _exit(1);
    }
    close(0);
    dup2(f, 0);
    close(f);
  }

  if(strlen(job->std[1])){
    f = open(job->std[1], O_WRONLY | O_TRUNC | O_CREAT, 0660);
    if(f == -1){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), job->std[1]);
      _exit(1);
    }
    close(1);
    if(dup2(f, 1) == -1){
      mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
      _exit(1);
    }
    close(f);
  }else{
    close(1);
    dup2(pp[1][1], 1);
  }

  if(strlen(job->std[2])){
    f = open(job->std[2], O_WRONLY | O_TRUNC | O_CREAT, 0660);
    if(f == -1){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), job->std[2]);
      _exit(1);
    }
    close(2);
    dup2(f, 2);
    close(f);
  }else{
    close(2);
    dup2(pp[2][1], 2);
  }
  close(pp[1][1]);
  close(pp[2][1]);
  job->ctl = pp[0][1];

  if(job->svr){
    /*===== remote execute process =====*/
    mtn_exec(mtn, job);
  }else{
    /*===== local exec process =====*/
    close(job->ctl);
    job->ctl = 0;
    execl("/bin/sh", "/bin/sh", "-c", job->cmd, NULL);
  }
  mtnlogger(mtn, 0, "[error] %s '%s'\n", strerror(errno), job->cmd);
  _exit(127);
}

int mtnexec_all(ARG arg)
{
  MTNSVR *s;
  MTNSVR *svr = getinfo(mtn);
  if(!svr){
    mtnlogger(mtn,0,"[mtnexec] error: node not found\n");
    return(-1);
  }
  for(s=svr;s;s=s->next){
    if(mtnexec_fork(cpsvr(s), arg) == -1){
      break;
    }
  }
  clrsvr(svr);
  return(s ? -1 : 0);
}

MTNSVR *mtnexec_hybrid(MTNSVR *svr)
{
  int count;
  if(ctx->mode != MTNEXECMODE_HYBRID){
    return(svr);
  }
  if(!svr){
    return(NULL);
  }
  count = getjobcount(1);
  if((svr->cnt.cld > count) && (ctx->cpu_num > count + 1)){
    svr = clrsvr(svr);
  }
  return(svr);
}

int mtnexec_remote(ARG arg)
{
  MTNSVR *svr;
  while(is_loop){
    if(!(svr = getinfo(mtn))){
      if((ctx->mode != MTNEXECMODE_HYBRID) && is_loop){
        mtnlogger(mtn, 0, "[mtnexec] error: node not found\n");
        return(-1);
      }
      if(getjobcount(1) == 0){
        return(mtnexec_fork(NULL, arg));
      }
    }else{
      if((svr = filtersvr(svr, 0))){
        svr = mtnexec_hybrid(svr);
        return(mtnexec_fork(svr, arg));
      }
    }
    while(is_loop && !mtnexec_poll() && getjobcount(0));
  }
  return(0);
}

int is_localbusy()
{
  int i;
  for(i=0;i<ctx->job_max;i++){
    if(ctx->job[i].svr == NULL){
      return(0);
    }
  }
  return(1);
}

int mtnexec(ARG arg)
{
  int r = 0;
  switch(ctx->mode){
    case MTNEXECMODE_LOCAL:
      r = mtnexec_fork(NULL, arg);
      break;
    case MTNEXECMODE_REMOTE:
    case MTNEXECMODE_HYBRID:
      r = mtnexec_remote(arg);
      break;
    case MTNEXECMODE_ALL0:
      r = mtnexec_all(arg);
      break;
    case MTNEXECMODE_ALL1:
      if(!(r = mtnexec_all(arg))){
        r = mtnexec_fork(NULL, arg);
      }
      break;
  }
  return(r);
}

//
// すべてのジョブが終了するまで待つ
//
int mtnexec_exit()
{
  int i;
  while(is_running() && is_loop){
    mtnexec_poll();
  }
  if(!ctx->signal){
    return(0);
  }
  for(i=0;i<ctx->job_max;i++){
    if(ctx->job[i].pid){
      kill(-(ctx->job[i].pid), ctx->signal);
    }
  }
  return(1);
}

ARG linearg()
{
  int i;
  ARG argl = newarg(ctx->arg_num);
  for(i=0;i<ctx->arg_num;i++){
    argl[i] = poparg(ctx->linearg);
    if(!argl[i]){
      clrarg(argl);
      return(NULL);
    }
  }
  return(argl);
}

void argexec()
{
  ARG argp;
  while(is_loop && (argp = ctx->linearg ? linearg() : readarg(0, ctx->arg_num))){
    if(mtnexec(argp) == -1){
      clrarg(argp);
      break;
    }      
    clrarg(argp);
  }
}

int init_pipe()
{
  pipe(ctx->fsig);
  fcntl(ctx->fsig[0], F_SETFD, FD_CLOEXEC);
  fcntl(ctx->fsig[1], F_SETFD, FD_CLOEXEC);
  fcntl(ctx->fsig[0], F_SETFL, O_NONBLOCK);
  ctx->efd = epoll_create(1);
  fcntl(ctx->efd, F_SETFD, FD_CLOEXEC);
  return(0);
}

int init_poll()
{
  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.ptr = NULL;
  epoll_ctl(ctx->efd, EPOLL_CTL_ADD, ctx->fsig[0], &ev);
  return(0);
}

int init(int argc, char *argv[])
{
  mtn = mtn_init(MODULE_NAME);
  ctx = calloc(1,sizeof(CTX));

  if(!mtn){
    return(-1);
  }
  if(!ctx){
    mtnlogger(NULL, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }

  mtn->mps_max = 512;
  mtn->logtype = 0;
  mtn->logmode = MTNLOG_STDERR;
  ctx->nobuf   = 1;
  ctx->text    = 1;
  ctx->delim   = newstr(" ");
  ctx->cpu_num = sysconf(_SC_NPROCESSORS_ONLN);
  ctx->job_max = sysconf(_SC_NPROCESSORS_ONLN);
  ctx->arg_num = isatty(fileno(stdin)) ? 0 : 1;
  ctx->cmdargs = parse(argc, argv);
  gettimeofday(&(ctx->polltv), NULL);

  if(init_pipe() == -1){
    return(-1);
  }
  if(init_poll() == -1){
    return(-1);
  }
  ctx->job = calloc(ctx->job_max, sizeof(MTNJOB));
  set_signal_handler();
  return(0);
}

int main(int argc, char *argv[])
{
  if(init(argc, argv) == -1){
    exit(1);
  }
  if(ctx->arg_num){
    argexec();
  }else{
    mtnexec(NULL);
  }
  return(mtnexec_exit());
}

