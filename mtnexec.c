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
  printf("   -M0          # local only (default)\n");
  printf("   -M1          # use remote and local\n");
  printf("   -M2          # use remote\n");
  printf("   -A0          # do all servers\n");
  printf("   -A1          # do all servers and local\n");
  printf("\n");
  printf("  OPTION\n");
  printf("   -0           # \n");
  printf("   -u           # \n");
  printf("   -g group     # \n");
  printf("   -j num       # \n");
  printf("   -N num       # \n");
  printf("   -m addr      # mcast addr(default:)\n");
  printf("   -p port      # TCP/UDP port(default: 6000)\n");
  printf("   -h           # help\n");
  printf("   -v           # version\n");
  printf("\n");
}

struct option *get_optlist()
{
  static struct option opt[]={
      {"help",    0, NULL, 'h'},
      {"version", 0, NULL, 'v'},
      {"stdin",   1, NULL, 'I'},
      {"push",    1, NULL, 'S'},
      {"pull",    0, NULL, 'P'},
      {0, 0, 0, 0}
    };
  return(opt);
}

ARG stdname(MTNJOB *job)
{
  ARG std = newarg(3);
  char path[PATH_MAX];
  if(ctx->stdin){
    std[0] = convarg(newstr(ctx->stdin), job->argl, NULL);
  }else{
    sprintf(path, "/dev/null");
    std[0] = newstr(path);
  }
  if(ctx->stdout){
    std[1] = convarg(newstr(ctx->stdout), job->argl, NULL);
  }else{
    sprintf(path, ".stdmtnexec.%d.out", job->pid);
    std[1] = newstr(path);
  }
  if(ctx->stderr){
    std[2] = convarg(newstr(ctx->stderr), job->argl, NULL);
  }else{
    sprintf(path, ".stdmtnexec.%d.err", job->pid);
    std[2] = newstr(path);
  }
  std[3] = NULL;
  return(std);
}

ARG parse(int argc, char *argv[])
{
  int i;
  int r;
  ARG args;
  if(argc < 2){
    usage();
    exit(0);
  }
  optind = 0;
  while((r = getopt_long(argc, argv, "+hvI:S:PuM:A:j:N:m:p:", get_optlist(), NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);

      case 'v':
        version();
        exit(0);

      case 'I':
        ctx->stdin = newstr(optarg);
        break;

      case 'S':
        ctx->push = splitstr(optarg, ",");
        break;

      case 'P':
        ctx->pull = 1;
        break;

      case 'u':
        ctx->stdout_direct = 1;
        break;

      case 'M':
        ctx->mode = atoi(optarg);
        if(ctx->mode >= MTNEXECMODE_ALL0){
          usage();
          exit(0);
        }
        break;
      case 'A':
        ctx->mode = atoi(optarg);
        if(ctx->mode < 2){
          ctx->mode += MTNEXECMODE_ALL0;
        }else{
          usage();
          exit(0);
        }
        break;
      case 'j':
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
  i = 0;
  args = newarg(argc - optind);
  while(optind < argc){
    if(strcmp(argv[optind], ":::") == 0){
      optind++;
      break;
    }
    args[i++] = newstr(argv[optind++]);
  }
  args[i] = NULL;

  if(optind < argc){
    i = 0;
    ctx->linearg = newarg(argc - optind);
    while(optind < argc){
      ctx->linearg[i++] = newstr(argv[optind++]);
    }
    ctx->linearg[i] = NULL;
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
  c->job_max = 1;
  c->stdout_direct = 0;
  c->stderr_direct = 1;
  c->arg_num = isatty(fileno(stdin)) ? 0 : 1;
  return(c);
}

void signal_handler(int n)
{
  switch(n){
    case SIGINT:
    case SIGTERM:
      is_loop = 0;
      break;
    case SIGPIPE:
      break;
  }
}

void set_signal_handler()
{
  struct sigaction sig;
  memset(&sig, 0, sizeof(sig));
  sig.sa_handler = signal_handler;
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

int jclose(MTNJOB *job)
{
  int r;
  int f;
  int status = 0;
  char buff[1024];

  if(job->pid){
    waitpid(job->pid, &status, 0);
  }
  if(job->efd > 0){
    close(job->efd);
    job->efd = 0;
  }
  if(job->rfd){
    epoll_ctl(ctx->efd, EPOLL_CTL_DEL, job->rfd, NULL);
    close(job->rfd);
    job->rfd = 0;
  }
  if(job->wfd){
    close(job->wfd);
    job->wfd = 0;
  }
  if(job->con > 0){
    close(job->con);
    job->con = 0;
  }
  if(job->std){
    if(!(ctx->stderr) && job->std[2] && strlen(job->std[2])){
      if((f = open(job->std[2], O_RDONLY))){
        while((r = read(f, buff, sizeof(buff))) > 0){
          fwrite(buff, r, 1, stderr);
        }
        close(f);
        unlink(job->std[2]);
      }
    }
    if(!(ctx->stdout) && job->std[1] && strlen(job->std[1])){
      if((f = open(job->std[1], O_RDONLY))){
        while((r = read(f, buff, sizeof(buff))) > 0){
          fwrite(buff, r, 1, stdout);
        }
        close(f);
        unlink(job->std[1]);
      }
    }
  }
  clrarg(job->std);
  clrarg(job->args);
  clrarg(job->argl);
  memset(job, 0, sizeof(MTNJOB));
  return(status);
}

MTNJOB *mtnexec_wait()
{
  int  i;
  int  r;
  char buff[256];
  MTNJOB *job;
  struct epoll_event ev;
  while(is_busy() && is_loop){
    r = epoll_wait(ctx->efd, &ev, 1, 1000);
    if(r == 1){
      job = ev.data.ptr;
      r = read(job->rfd, buff, sizeof(buff));
      if(r == 0){
        epoll_ctl(ctx->efd, EPOLL_CTL_DEL, job->rfd, NULL);
        jclose(job);
        return(job);
      }
    }
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

int mtnexec_fork(MTNJOB *job)
{
  pid_t pid;
  int rp[2];
  int wp[2];

  if(!job){
    return(-1);
  }
  if(pipe(rp) == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(pipe(wp) == -1){
    close(rp[0]);
    close(rp[1]);
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  pid = fork();
  if(pid == -1){
    close(rp[0]);
    close(rp[1]);
    close(wp[0]);
    close(wp[1]);
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(pid){
    close(rp[1]);
    close(wp[0]);
    job->pid   = pid;
    job->rfd = rp[0];
    job->wfd = wp[1];
    job->std   = stdname(job);
  }else{
    close(rp[0]);
    close(wp[1]);
    job->pid   = 0;
    job->rfd = wp[0];
    job->wfd = rp[1];
  }
  return(0);
}

MTNJOB *copyjob(MTNJOB *dst, MTNJOB *src)
{
  if(dst){
    memcpy(dst, src, sizeof(MTNJOB));
    dst->std  = copyarg(src->std);
    dst->args = copyarg(src->args);
    dst->argl = copyarg(src->argl);
  }
  return(dst);
}

int mtnexec_local(MTNJOB *job)
{
  STR cmd;
  int status;
  struct epoll_event ev;
  job = copyjob(mtnexec_wait(), job);
  if(mtnexec_fork(job) == -1){
    return(-1);
  }
  if(job->pid){
    ev.data.ptr = job;
    ev.events   = EPOLLIN;
    epoll_ctl(ctx->efd, EPOLL_CTL_ADD, job->rfd, &ev);
    write(job->wfd, job->std[0], strlen(job->std[0]) + 1);
    write(job->wfd, job->std[1], strlen(job->std[1]) + 1);
    write(job->wfd, job->std[2], strlen(job->std[2]) + 1);
    return(0);
  }

  //===== child process =====
  job->std = readarg(job->rfd, 3);
  job->pid = fork();
  if(job->pid == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    close(job->rfd);
    close(job->wfd);
    _exit(0);
  }
  if(job->pid){
    waitpid(job->pid, &status, 0);
    close(job->rfd);
    close(job->wfd);
    _exit(0);
  }

  //===== exec process =====
  close(job->rfd);
  close(job->wfd);
  if(job->std){
    close(0);
    open(job->std[0], O_RDONLY);
    if(!ctx->stdout_direct){
      close(1);
      open(job->std[1], O_WRONLY | O_APPEND | O_CREAT, 0660);
    }
    if(!ctx->stderr_direct){
      close(2);
      open(job->std[2], O_WRONLY | O_APPEND | O_CREAT, 0660);
    }
  }

  cmd = joinarg(fullargs(job->args, job->argl));
  if(!cmd){
    mtnlogger(mtn, 0, "[error] %s: cmd is null\n", __func__);
    _exit(127);
  }
  status = system(cmd);
  if(status == -1){
    mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), cmd);
    _exit(127);
  }
  _exit(WEXITSTATUS(status)); 
}

int mtnexec_remote(MTNJOB *job)
{
  int f;
  int status = 0;
  struct epoll_event ev;
  job = copyjob(mtnexec_wait(), job);
  if(mtnexec_fork(job) == -1){
    return(-1);
  }
  if(job->pid){
    ev.data.ptr = job;
    ev.events   = EPOLLIN;
    epoll_ctl(ctx->efd, EPOLL_CTL_ADD, job->rfd, &ev);
    write(job->wfd, job->std[0], strlen(job->std[0]) + 1);
    write(job->wfd, job->std[1], strlen(job->std[1]) + 1);
    write(job->wfd, job->std[2], strlen(job->std[2]) + 1);
    return(0);
  }

  //===== child process =====
  job->in  = 0;
  job->out = 1;
  job->err = 2;
  if((job->std = readarg(job->rfd, 3))){
    if(strlen(job->std[0])){
      f = open(job->std[0], O_RDONLY);
      if(f == -1){
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), job->std[0]);
        _exit(1);
      }
      job->in = f;
    }
    if(!ctx->stdout_direct){
      f = open(job->std[1], O_WRONLY | O_APPEND | O_CREAT, 0660);
      if(f == -1){
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), job->std[1]);
        _exit(1);
      }
      job->out = f;
    }
    if(!ctx->stderr_direct){
      f = open(job->std[2], O_WRONLY | O_APPEND | O_CREAT, 0660);
      if(f == -1){
        mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), job->std[2]);
        _exit(1);
      }
      job->err = f;
    }
  }
  mtn_exec(mtn, job);
  close(job->rfd);
  close(job->wfd);
  _exit(WEXITSTATUS(status)); 
}

void mtnexec_remote_all(MTNJOB *job)
{
  MTNSVR *svr;
  for(svr=ctx->svr;svr;svr=svr->next){
    job->svr = svr;
    mtnexec_remote(job);
  }
}

void mtnexec_remote_one(MTNJOB *job)
{
  job->svr = ctx->svr;
  mtnexec_remote(job);
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
  MTNJOB job;
  memset(&job, 0, sizeof(job));
  job.uid  = getuid();
  job.gid  = getgid();
  job.args = copyarg(args);
  job.argl = copyarg(argl);
  switch(ctx->mode){
    case MTNEXECMODE_LOCAL:
      mtnexec_local(&job);
      break;
    case MTNEXECMODE_REMOTE:
      mtnexec_remote_one(&job);
      break;
    case MTNEXECMODE_HYBRID:
      if(is_localbusy()){
        mtnexec_remote_one(&job);
      }else{
        mtnexec_local(&job);
      }
      break;
    case MTNEXECMODE_ALL0:
      mtnexec_remote_all(&job);
      break;
    case MTNEXECMODE_ALL1:
      mtnexec_local(&job);
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
  int  r;
  char buff[256];
  struct epoll_event ev;
  MTNJOB *job;
  while(is_running() && is_loop){
    r = epoll_wait(ctx->efd, &ev, 1, 1000);
    if(r == 1){
      job = ev.data.ptr;
      r = read(job->rfd, buff, sizeof(buff));
      if(r == 0){
        jclose(job);
      }
    }
  }
  return(0);
}

STR poparg(ARG args)
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
  ARG argl = NULL;
  ARG args = NULL;
  set_signal_handler();
  ctx = mtnexec_init();
  mtn = mtn_init(MODULE_NAME);
  mtn->logmode = MTNLOG_STDERR;
  args = parse(argc, argv);
  ctx->svr = (ctx->mode == MTNEXECMODE_LOCAL) ? NULL : mtn_info(mtn);
  ctx->efd = epoll_create(ctx->job_max);
  ctx->job = calloc(ctx->job_max, sizeof(MTNJOB));
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

