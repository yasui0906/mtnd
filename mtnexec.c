/*
 * mtnexec.c
 * Copyright (C) 2011 KLab Inc.
 */
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#include <mtn.h>
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
  printf("   -g group           # \n");
  printf("   -P num             # \n");
  printf("   -N num             # \n");
  printf("   -m addr            # mcast addr(default:)\n");
  printf("   -p port            # TCP/UDP port(default: 6000)\n");
  printf("   -h                 # help\n");
  printf("   --version          # version\n");
  printf("   --stdin=path       # \n");
  printf("   --stdout=path      # \n");
  printf("   --stderr=path      # \n");
  printf("   --put=path,path,,, # \n");
  printf("   --get=path,path,,, # \n");
  printf("\n");
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
  mtn_info_clrcache(mtn); 
  svrlist = mtn_info(mtn);
  for(s=svrlist;s;s=s->next){
    if(is_execute(s)){
      members = pushsvr(members, s);
    }
  }
  clrsvr(svrlist);
  return(members);
}

void info()
{
  MTNSVR *s;
  MTNSVR *svrlist  = getinfo();
  uint32_t node    = 0;
  uint32_t cpu_num = 0;
  uint64_t memsize = 0;
  uint64_t memfree = 0;

  for(s=svrlist;s;s=s->next){
    node++;
    cpu_num += s->cnt.cpu;
    memsize += s->memsize/1024/1024;
    memfree += s->memfree/1024/1024;
    printf("%5s: ",       s->host);
    printf("ORD=%03d ",   s->order);
    printf("CPU=%02d ",   s->cnt.cpu);
    printf("LA=%d.%02d ", s->loadavg / 100, s->loadavg % 100);
    printf("MEM=%luM ",   s->memsize/1024/1024);
    printf("FREE=%luM ",  s->memfree/1024/1024);
    printf("PS=%d ",      s->cnt.prc);
    printf("VSZ=%luK ",   s->vsz/1024);
    printf("RES=%luK ",   s->res/1024);
    printf("MLC=%d ",     s->cnt.mem);
    printf("TSK=%d ",     s->cnt.tsk);
    printf("SVR=%d ",     s->cnt.svr);
    printf("DIR=%d ",     s->cnt.dir);
    printf("STA=%d ",     s->cnt.sta);
    printf("STR=%d ",     s->cnt.str);
    printf("ARG=%d ",     s->cnt.arg);
    printf("CLD=%d ",     s->cnt.cld);
    printf("\n");
  }
  if(!node){
    printf("node not found");
  }else{
    printf("TOTAL: ");
    printf("%dCPU(%dnode) ", cpu_num, node);
    if(memsize){
      printf("MEM=%luM/%luM(%lu%%Free) ",  memfree, memsize, memfree * 100 / memsize);
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
      {"stdin",   1, NULL, 'I'},
      {"stdout",  1, NULL, 'O'},
      {"stderr",  1, NULL, 'E'},
      {"put",     1, NULL, 'S'},
      {"get",     1, NULL, 'G'},
      {0, 0, 0, 0}
    };
  return(opt);
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
    std[0] = convarg(newstr(ctx->stdin), job->argl);
  }else{
    std[0] = newstr("/dev/null");
  }
  if(ctx->stdout){
    std[1] = convarg(newstr(ctx->stdout), job->argl);
  }else{
    std[1] = newstr("");
  }
  if(ctx->stderr){
    std[2] = convarg(newstr(ctx->stderr), job->argl);
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
  while((r = getopt_long(argc, argv, "+hVvnbig:j:I:RALP:N:m:p:", get_optlist(), NULL)) != -1){
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

STR *readarg(int f, int n)
{
  int  i;
  int  r;
  int  len = 0;
  char buff[1024];
  STR *arg = newarg(n);
  for(i=0;i<n;i++){
    len = 0;
    while(is_loop){
      r = read(f, buff + len, 1);
      if(r == 0){
        if(len){
          buff[len] = 0;
          break;
        }
        errno = 0;
        for(r=0;r<n;r++){
          free(arg[r]);
          arg[r] = NULL;
        }
        free(arg);
        return(NULL);
      }
      if(r == -1){
        if(errno == EINTR){
          errno = 0;
          continue;
        }
        return(NULL);
      }
      if(buff[len] == '\r' || buff[len] == '\n' || buff[len] == 0){
        buff[len] = 0;
        if(len == 0){
          continue;
        }
        break;
      }
      len++; 
    }
    buff[len] = 0;
    arg[i] = newstr(buff);
  }
  arg[i] = NULL;
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

int mtnexec_stdbuff(MTNJOB *job)
{
  int r;
  char buff[1024];
  r = read(job->pfd, buff, sizeof(buff));
  if(r == -1){
    return(-1);
  }
  if(r == 0){
    epoll_ctl(ctx->efd, EPOLL_CTL_DEL, job->pfd, NULL);
    close(job->pfd);
    job->pfd = 0;
    return(0);
  }
  if(job->stdout.datasize + r > job->stdout.buffsize){
    job->stdout.stdbuff = realloc(job->stdout.stdbuff, job->stdout.buffsize + sizeof(buff));
    job->stdout.buffsize += sizeof(buff);
  }
  memcpy(job->stdout.stdbuff + job->stdout.datasize, buff, r);
  job->stdout.datasize += r;
  return(r);
}

int jclose(MTNJOB *job)
{
  if(job->pfd){
    epoll_ctl(ctx->efd, EPOLL_CTL_DEL, job->pfd, NULL);
    while(mtnexec_stdbuff(job) > 0);
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

void mtnexec_poll()
{
  int i;
  int r;
  pid_t pid;
  int status;
  char data;
  struct epoll_event ev;
  static int wtime = 200;
  struct timeval  polltv;
  struct timeval keikatv;

  gettimeofday(&polltv, NULL);
  timersub(&polltv, &(ctx->polltv), &keikatv);
  if(wtime < (keikatv.tv_sec * 1000 + keikatv.tv_usec / 1000)){
    memcpy(&(ctx->polltv), &polltv, sizeof(struct timeval));
    ctx->cpu_use = scheprocess(mtn, ctx->job, ctx->job_max, ctx->cpu_lim, ctx->cpu_num);
    wtime = getwaittime(ctx->job, ctx->job_max);
  }

  r = epoll_wait(ctx->efd, &ev, 1, wtime);
  if(r == -1){
    if(errno != EINTR){
      mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
      return;
    }
  }
  if((r == 1) && ev.data.ptr){
    r = mtnexec_stdbuff(ev.data.ptr);
    return;
  }

  read(ctx->fsig[0], &data, sizeof(data));
  while((pid = waitpid(-1, &status, WNOHANG)) > 0){
    for(i=0;i<ctx->job_max;i++){
      if(pid == ctx->job[i].pid){
        jclose(&(ctx->job[i]));
        break;
      }
    }
  }
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

MTNJOB *copyjob(MTNJOB *dst, MTNJOB *src)
{
  if(dst){
    memcpy(dst, src, sizeof(MTNJOB));
    dst->std    = copyarg(src->std);
    dst->cmd    = newstr(src->cmd);
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

int mtnexec_fork(MTNJOB *job)
{
  int f;
  int pp[2];
  struct epoll_event ev;

  job = copyjob(mtnexec_wait(), job); // プロセス数の上限に達していたらここで待機する
  if(!job){
    return(-1);
  }
  job->cid = -1;
  job->std = stdname(job);
  job->putarg = cpconvarg(ctx->putarg, ctx->conv ? job->argl : NULL);
  job->getarg = cpconvarg(ctx->getarg, ctx->conv ? job->argl : NULL);

  if(ctx->dryrun){
    mtnexec_dryrun(job);
    jclose(job);
    return(0);
  }
  if(ctx->verbose){
    mtnexec_verbose(job);
  }
  pipe(pp);
  gettimeofday(&(job->start), NULL);
  job->pid = fork();
  if(job->pid == -1){
    job->pid = 0;
    close(pp[0]);
    close(pp[1]);
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(job->pid){
    close(pp[1]);
    fcntl(job->pfd, F_SETFD, FD_CLOEXEC);
    job->cct   = 1;
    job->pstat = calloc(1, sizeof(MTNPROCSTAT));
    job->pstat[0].pid = job->pid;
    job->pfd = pp[0];
    ev.data.ptr = job;
    ev.events   = EPOLLIN;
    if(epoll_ctl(ctx->efd, EPOLL_CTL_ADD, job->pfd, &ev) == -1){
      mtnlogger(mtn, 0, "[error] %s: epoll_ctl %s\n", __func__, strerror(errno));
    }
    return(0);
  }

  //===== execute process =====
  setpgid(0,0);
  close(pp[0]);
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
  }else if(!ctx->nobuf){
    close(1);
    dup2(pp[1], 1);
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
  }else if(!ctx->nobuf){
    close(2);
    dup2(pp[1], 2);
  }
  close(pp[1]);

  if(job->svr){
    /*===== remote execute process =====*/
    mtn_exec(mtn, job);
  }else{
    /*===== local exec process =====*/
    execl("/bin/sh", "/bin/sh", "-c", job->cmd, NULL);
  }
  mtnlogger(mtn, 0, "[error] %s '%s'\n", strerror(errno), job->cmd);
  _exit(127);
}

void mtnexec_all(MTNJOB *job)
{
  MTNSVR *svr;
  for(svr=ctx->svr;svr;svr=svr->next){
    job->svr = cpsvr(svr);
    mtnexec_fork(job);
  }
}

MTNSVR *filtersvr_loadavg(MTNSVR *svr)
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

MTNSVR *filtersvr_cnt_job(MTNSVR *svr)
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
  clrsvr(svr);
  return(r);
}

int filtersvr_list_limit(int *list, int count, int level)
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

static int cmp1(const void *p1, const void *p2)
{
  int v1 = *(int *)p1;
  int v2 = *(int *)p2;
  if(v1 == v2){
    return(0);
  }
  return((v1 > v2) ? 1 : -1);
}

static int cmp2(const void *p1, const void *p2)
{
  int v1 = *(int *)p1;
  int v2 = *(int *)p2;
  if(v1 == v2){
    return(0);
  }
  return((v1 < v2) ? 1 : -1);
}

MTNSVR *filtersvr_cnt_prc(MTNSVR *svr)
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
  qsort(list, count, sizeof(int), cmp1);

  do{
    r = clrsvr(r);
    limit = filtersvr_list_limit(list, count, level++);
    for(s=svr;s;s=s->next){
      if(!limit || (s->cnt.prc <= limit)){
        r = pushsvr(r, s);
      }
    }
    r = filtersvr_cnt_job(r);
  }while(limit && !r);
  free(list);
  clrsvr(svr);
  return(r);
}

MTNSVR *filtersvr_memfree(MTNSVR *svr)
{
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
    list[count] = (int)(s->memfree / 1024 / 1024);
    count++;
  }
  qsort(list, count, sizeof(int), cmp2);
  limit = filtersvr_list_limit(list, count, 0);
  for(s=svr;s;s=s->next){
    if(!limit || (s->cnt.prc <= limit)){
      r = pushsvr(r, s);
    }
  }
  free(list);
  clrsvr(svr);
  return(r);
}

MTNSVR *filtersvr_order(MTNSVR *svr)
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

MTNSVR *filtersvr()
{
  MTNSVR *s;
  s = getinfo(mtn);
  s = filtersvr_loadavg(s); // LAが1以上のノードを除外する
  s = filtersvr_cnt_prc(s); // プロセス数が少ないノードを抽出する
  s = filtersvr_memfree(s); // 空きメモリが多いノードを抽出する
  s = filtersvr_order(s);   // 応答速度が一番速かったノードを選択する
  return(s);
}

void mtnexec_hybrid(MTNJOB *job)
{
  int count;
  if(ctx->mode != MTNEXECMODE_HYBRID){
    return;
  }
  if(!job || !job->svr){
    return;
  }
  count = getjobcount(1);
  if((job->svr->cnt.cld > count) && (ctx->cpu_num > count + 1)){
    job->svr = clrsvr(job->svr);
  }
}

void mtnexec_remote(MTNJOB *job)
{
  struct timeval savetv;
  struct timeval polltv;
  struct timeval keiktv;
  while(is_loop){
    gettimeofday(&savetv, NULL);
    if((job->svr = filtersvr())){
      mtnexec_hybrid(job);
      mtnexec_fork(job);
      break;
    }
    do{
      mtnexec_poll();
      gettimeofday(&polltv, NULL);
      timersub(&polltv, &savetv, &keiktv);
    }while(TVMSEC(keiktv) < 1000);
  }
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

void mtnexec(ARG argp)
{
  MTNJOB job;
  static int id = 0;
  memset(&job, 0, sizeof(job));
  job.id   = id++;
  job.uid  = getuid();
  job.gid  = getgid();
  job.cid  = -1;
  job.lim  = ctx->cpu_lim;
  job.conv = ctx->conv;
  job.args = copyarg(ctx->cmdargs);
  job.argl = copyarg(argp);
  job.argc = cmdargs(&job);
  job.cmd  = joinarg(job.argc, " ");

  if(is_empty(job.cmd)){
    jclose(&job);
    return;
  }    
  switch(ctx->mode){
    case MTNEXECMODE_LOCAL:
      mtnexec_fork(&job);
      break;
    case MTNEXECMODE_REMOTE:
    case MTNEXECMODE_HYBRID:
      mtnexec_remote(&job);
      break;
    case MTNEXECMODE_ALL0:
      mtnexec_all(&job);
      break;
    case MTNEXECMODE_ALL1:
      mtnexec_fork(&job);
      mtnexec_all(&job);
      break;
  }
  jclose(&job);
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
  if(ctx->signal){
    for(i=0;i<ctx->job_max;i++){
      if(ctx->job[i].pid){
        kill(-(ctx->job[i].pid), ctx->signal);
      }
    }
  }
  return(0);
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
    mtnexec(argp);
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

  mtn->logtype = 0;
  mtn->logmode = MTNLOG_STDERR;
  ctx->nobuf   = 1;
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
  ctx->svr = (ctx->mode == MTNEXECMODE_LOCAL) ? NULL : getinfo(mtn);
  if(!ctx->svr){
    if(ctx->mode == MTNEXECMODE_REMOTE){
      mtnlogger(mtn, 0, "[error] node not found\n");
      return(-1);
    }
    if(ctx->mode == MTNEXECMODE_ALL0){
      mtnlogger(mtn, 0, "[error] node not found\n");
      return(-1);
    }
  }
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

