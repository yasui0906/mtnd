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
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/wait.h>
#include <fcntl.h>
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
  printf("usage: %s [-M0|-M1|-M2|-A0|-A1] [OPTION] [command [initial-arguments]]\n", MODULE_NAME);
  printf("\n");
  printf("  MODE\n");
  printf("   -M0                # local only (default)\n");
  printf("   -M1                # use remote and local\n");
  printf("   -M2                # use remote\n");
  printf("   -A0                # do all servers\n");
  printf("   -A1                # do all servers and local\n");
  printf("\n");
  printf("  OPTION\n");
  printf("   -0                 # \n");
  printf("   -i                 # replace string\n");
  printf("   -t                 # \n");
  printf("   -n                 # dryrun\n");
  printf("   -b                 # \n");
  printf("   -g group           # \n");
  printf("   -P num             # \n");
  printf("   -N num             # \n");
  printf("   -m addr            # mcast addr(default:)\n");
  printf("   -p port            # TCP/UDP port(default: 6000)\n");
  printf("   -h                 # help\n");
  printf("   -v                 # version\n");
  printf("   --stdin=path       # \n");
  printf("   --stdout=path      # \n");
  printf("   --stderr=path      # \n");
  printf("   --put=path,path,,, # \n");
  printf("   --get=path,path,,, # \n");
  printf("\n");
}

struct option *get_optlist()
{
  static struct option opt[]={
      {"help",    0, NULL, 'h'},
      {"version", 0, NULL, 'v'},
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
  while((r = getopt_long(argc, argv, "+hvtnbiI:M:A:P:N:m:p:", get_optlist(), NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'v':
        version();
        exit(0);

      case 't':
        ctx->verbose = 1;
        break;

      case 'n':
        ctx->verbose = 1;
        ctx->dryrun  = 1;
        break;

      case 'i':
        ctx->conv = 1;
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

      case 'M':
        switch(atoi(optarg)){
          case 0:
            ctx->mode = MTNEXECMODE_LOCAL;
            break;
          case 1:
            ctx->mode = MTNEXECMODE_HYBRID;
            break;
          case 2:
            ctx->mode = MTNEXECMODE_REMOTE;
            break;
          default:
            usage();
            exit(0);
        }
        break;

      case 'A':
        switch(atoi(optarg)){
          case 0:
            ctx->mode = MTNEXECMODE_ALL0;
            break;
          case 1:
            ctx->mode = MTNEXECMODE_ALL1;
            break;
          default:
            usage();
            exit(0);
        }
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

CTX *mtnexec_init()
{
  CTX *c = calloc(1, sizeof(CTX));
  c->nobuf   = 1;
  c->job_max = 1;
  c->arg_num = isatty(fileno(stdin)) ? 0 : 1;
  return(c);
}

void signal_handler(int n, siginfo_t *info, void *ucs)
{
  char data;
  switch(n){
    case SIGINT:
    case SIGTERM:
      is_loop = 0;
      mtn_break();
      break;
    case SIGPIPE:
      break;
    case SIGCHLD:
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
      if(buff[len] <= ' '){
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

int jflush(MTNJOB *job)
{
  int r;
  size_t size = 0;
  while(job->stdout.datasize - size){
    r = write(1, job->stdout.stdbuff + size, job->stdout.datasize - size);
    if(r == -1){
      return(-1);
    }
    size += r;
  }
  free(job->stdout.stdbuff);
  job->stdout.stdbuff  = NULL;
  job->stdout.buffsize = 0;
  job->stdout.datasize = 0;;
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
  if(job->efd > 0){
    close(job->efd);
    job->efd = 0;
  }
  if(job->pfd){
    epoll_ctl(ctx->efd, EPOLL_CTL_DEL, job->pfd, NULL);
    while(mtnexec_stdbuff(job) > 0);
    close(job->pfd);
    job->pfd = 0;
  }
  if(job->con > 0){
    close(job->con);
    job->con = 0;
  }
  jflush(job);
  clrstr(job->cmd);
  clrarg(job->std);
  clrarg(job->putarg);
  clrarg(job->getarg);
  clrarg(job->args);
  clrarg(job->argl);
  clrarg(job->argc);
  memset(job, 0, sizeof(MTNJOB));
  return(0);
}

void mtnexec_poll()
{
  int i;
  pid_t pid;
  int status;
  char data;
  struct epoll_event ev;

  if(epoll_wait(ctx->efd, &ev, 1, 1000) != 1){
    return;
  }
  if(ev.data.ptr){
    mtnexec_stdbuff(ev.data.ptr);
    return;
  }
  read(ctx->fsig[0], &data, 1);
  while((pid = waitpid(-1, &status, WNOHANG)) != -1){
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

  pipe(pp);
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
    job->pfd = pp[0];
    fcntl(job->pfd, F_SETFD, FD_CLOEXEC);
    ev.data.ptr = job;
    ev.events   = EPOLLIN;
    if(epoll_ctl(ctx->efd, EPOLL_CTL_ADD, job->pfd, &ev) == -1){
      mtnlogger(mtn, 0, "[error] %s: epoll_ctl %s\n", __func__, strerror(errno));
    }
    return(0);
  }

  //===== child process =====
  close(pp[0]);
  job->in  = 0;
  job->out = 1;
  job->err = 2;
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
    f = open(job->std[1], O_WRONLY | O_APPEND | O_CREAT, 0660);
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
    close(pp[1]);
  }
  if(strlen(job->std[2])){
    f = open(job->std[2], O_WRONLY | O_APPEND | O_CREAT, 0660);
    if(f == -1){
      mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), job->std[2]);
      _exit(1);
    }
    close(2);
    dup2(f, 2);
    close(f);
  }

  if(local){
    //===== local exec process =====
    execlp("sh", "sh", "-c", job->cmd, NULL);
    mtnlogger(mtn, 0, "[error] %s %s\n", strerror(errno), job->cmd);
    _exit(127);
  }
  //===== remote exec process =====
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
    job->svr = svr;
    mtnexec_fork(job, 0);
  }
}

void mtnexec_remote_one(MTNJOB *job)
{
  job->svr = ctx->svr;
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

void mtnexec(ARG args, ARG argl)
{
  static int id = 0;
  MTNJOB job;
  memset(&job, 0, sizeof(job));
  job.id   = id++;
  job.uid  = getuid();
  job.gid  = getgid();
  job.conv = ctx->conv;
  job.args = copyarg(args);
  job.argl = copyarg(argl);
  job.argc = cmdargs(&job);
  job.cmd  = joinarg(job.argc);

  if(is_empty(job.cmd)){
    jclose(&job);
    return;
  }    
  if(ctx->verbose){
    fprintf(stderr, "command: %s\n", job.cmd);
  }
  if(ctx->dryrun){
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
  while(is_running() && is_loop){
    mtnexec_poll();
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

int main(int argc, char *argv[])
{
  struct epoll_event ev;
  ARG argl = NULL;
  ARG args = NULL;
  ctx = mtnexec_init();
  mtn = mtn_init(MODULE_NAME);
  mtn->logmode = MTNLOG_STDERR;
  set_signal_handler();
  args = parse(argc, argv);
  ctx->svr = (ctx->mode == MTNEXECMODE_LOCAL) ? NULL : mtn_info(mtn);
  ctx->job = calloc(ctx->job_max, sizeof(MTNJOB));
  ctx->efd = epoll_create(ctx->job_max);
  pipe(ctx->fsig);
  fcntl(ctx->efd,     F_SETFD, FD_CLOEXEC);
  fcntl(ctx->fsig[0], F_SETFD, FD_CLOEXEC);
  fcntl(ctx->fsig[1], F_SETFD, FD_CLOEXEC);
  ev.events   = EPOLLIN;
  ev.data.ptr = NULL;
  epoll_ctl(ctx->efd, EPOLL_CTL_ADD, ctx->fsig[0], &ev);
  if(ctx->svr == NULL){
    switch(ctx->mode){
      case MTNEXECMODE_REMOTE:
      case MTNEXECMODE_ALL0:
        mtnlogger(mtn, 0, "[error] node not found\n");
        exit(0);
    }
  }
  if(ctx->arg_num){
    if(ctx->linearg){
      while((argl = linearg()) && is_loop){
        mtnexec(args, argl);
        clrarg(argl);
      }
    }else{
      while((argl = readarg(0, ctx->arg_num)) && is_loop){
        mtnexec(args, argl);
        clrarg(argl);
      }
    }
  }else{
    mtnexec(args, NULL);
  }
  clrarg(args);
  return(mtnexec_exit());
}

