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

MTNSVR *getinfo()
{
  MTNSVR *s;
  MTNSVR *members = NULL;
  MTNSVR *svrlist = mtn_info(mtn);
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
  int node    = 0;
  int cpu_num = 0;
  uint64_t memsize = 0;
  uint64_t memfree = 0;
  MTNSVR *svrlist  = getinfo();
  MTNSVR *s;
  for(s=svrlist;s;s=s->next){
    node++;
    cpu_num += s->cpu_num;
    memsize += s->memsize/1024/1024;
    memfree += s->memfree/1024/1024;
    printf("%5s: ",       s->host);
    printf("CPU=%d ",     s->cpu_num);
    printf("LA=%d.%02d ", s->loadavg / 100, s->loadavg % 100);
    printf("MEM=%luM ",   s->memsize/1024/1024);
    printf("FREE=%luM ",  s->memfree/1024/1024);
    printf("PS=%d ",      s->pscount);
    printf("VSZ=%luK ",   s->vsz/1024);
    printf("RES=%luK ",   s->res/1024);
    printf("CNT=%d ",     s->malloccnt);
    printf("TSK=%d ",     s->taskcnt);
    printf("SVR=%d ",     s->svrcnt);
    printf("DIR=%d ",     s->dircnt);
    printf("STA=%d ",     s->statcnt);
    printf("STR=%d ",     s->strcnt);
    printf("ARG=%d ",     s->argcnt);
    printf("CLD=%d ",     s->cldcnt);
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

int mtnexec_fork(MTNJOB *job, int local)
{
  int i;
  int f;
  int pp[2];
  struct epoll_event ev;

  job = copyjob(mtnexec_wait(), job);
  if(!job){
    return(-1);
  }
  job->cid = -1;
  job->std = stdname(job);
  job->putarg = newarg(0);
  job->getarg = newarg(0);

  if(ctx->putarg){
    for(i=0;ctx->putarg[i];i++){
      job->putarg = addarg(job->putarg, ctx->putarg[i]);
      if(ctx->conv){
        job->putarg[i] = convarg(job->putarg[i], job->argl);
      }
    }
  }
  if(ctx->getarg){
    for(i=0;ctx->getarg[i];i++){
      job->getarg = addarg(job->getarg, ctx->getarg[i]);
      if(ctx->conv){
        job->getarg[i] = convarg(job->getarg[i], job->argl);
      }
    }
  }

  if(ctx->verbose){
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
  if(ctx->dryrun){
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
    jclose(job);
    return(0);
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

  //===== local execute process =====
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
    dup2(f, 1);
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
  dup2(pp[1], 1);

  if(local){
    //===== local exec process =====
    execl("/bin/sh", "/bin/sh", "-c", job->cmd, NULL);
    mtnlogger(mtn, 0, "[error] %s %s\n", strerror(errno), job->cmd);
    _exit(127);
  }

  //===== remote execute process =====
  if(mtn_exec(mtn, job) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    _exit(127); 
  }
  _exit(0);
}

void mtnexec_remote_all(MTNJOB *job)
{
  MTNSVR *svr;
  for(svr=ctx->svr;svr;svr=svr->next){
    job->svr = cpsvr(svr);
    mtnexec_fork(job, 0);
  }
}

void mtnexec_remote_one(MTNJOB *job)
{
  int i;
  int l1;
  int l2;
  int p1;
  int p2;
  MTNSVR *s;
  while(is_loop){
    for(s=ctx->svr;s;s=s->next){
      for(i=0;i<ctx->job_max;i++){
        if(ctx->job[i].pid && !cmpsvr(ctx->job[i].svr, s)){
          break;
        }
      }
      if(i == ctx->job_max){
        if(!job->svr){
          job->svr = s;
        }else{
          l1 = job->svr->loadavg / job->svr->cpu_num;
          l2 = s->loadavg / s->cpu_num;
          p1 = job->svr->pscount;
          p2 = s->pscount;
          if(l1 == l2){
            if(p1 > p2){
              job->svr = s;
            } 
          }else if(l1 > l2){
            job->svr = s;
          }
        }
      }
    }
    if(job->svr){
      break;
    }
    mtnexec_poll();
    clrsvr(ctx->svr);
    ctx->svr = getinfo(mtn);
  }
  job->svr = cpsvr(job->svr);
  mtnexec_fork(job, 0);
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
      mtnexec_fork(&job, 1);
      break;
    case MTNEXECMODE_REMOTE:
      mtnexec_remote_one(&job);
      break;
    case MTNEXECMODE_HYBRID:
      if(is_localbusy()){
        mtnexec_remote_one(&job);
      }else{
        mtnexec_fork(&job, 1);
      }
      break;
    case MTNEXECMODE_ALL0:
      mtnexec_remote_all(&job);
      break;
    case MTNEXECMODE_ALL1:
      mtnexec_fork(&job, 1);
      mtnexec_remote_all(&job);
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

