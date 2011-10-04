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
  printf("usage: %s [OPTION]\n", MODULE_NAME);
  printf("\n");
  printf("  OPTION\n");
  printf("   -h       # help\n");
  printf("   -v       # version\n");
  printf("   -A       # all server\n");
  printf("   -j num   #\n");
  printf("   -N num   # stdin input arg per exec (default: 1)\n");
  printf("   -m addr  # mcast addr\n");
  printf("   -p port  # TCP/UDP port(default: 6000)\n");
  printf("\n");
}

struct option *get_optlist()
{
  static struct option opt[]={
      {"help",    0, NULL, 'h'},
      {"version", 0, NULL, 'v'},
      {0, 0, 0, 0}
    };
  return(opt);
}

char **parse(int argc, char *argv[])
{
  int i;
  int r;
  char **args;
  if(argc < 2){
    usage();
    exit(0);
  }
  optind = 0;
  while((r = getopt_long(argc, argv, "+hvAj:N:m:p:", get_optlist(), NULL)) != -1){
    switch(r){
      case 'h':
        usage();
        exit(0);
      case 'v':
        version();
        exit(0);
      case 'A':
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
  args = calloc(argc - optind + 1, sizeof(char *));
  while(optind < argc){
    args[i++] = newstr(argv[optind++]);
  }
  args[i] = NULL;
  return(args);
}

CTX *mtnexec_init()
{
  CTX *c = calloc(1, sizeof(CTX));
  c->job_max = 1;
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

char *mtnexec_dotname(char *str)
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

char *mtnexec_basename(char *str)
{
  char buff[PATH_MAX];
  strcpy(buff, str);
  return(modstr(str, basename(buff)));
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

char **readarg()
{
  int  i;
  int  r;
  int  fd  = 0;
  int  len = 0;
  char buff[1024];
  char **arg = calloc(ctx->arg_num + 1, sizeof(char *));
  for(i=0;i<ctx->arg_num;i++){
    len = 0;
    while(is_loop){
      r = read(fd, buff + len, 1);
      if(r == 0){
        if(len){
          buff[len] = 0;
          break;
        }
        errno = 0;
        for(r=0;r<ctx->arg_num;r++){
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
  int status = 0;
  if(job->pid){
    waitpid(job->pid, &status, 0);
  }
  if(job->pipe){
    epoll_ctl(ctx->e, EPOLL_CTL_DEL, job->pipe, NULL);
    close(job->pipe);
  }
  if(job->sock){
    close(job->sock);
  }
  job->pid  = 0;
  job->pipe = 0;
  job->sock = 0;
  job->svr  = NULL;
  return(status);
}

MTNJOB *mtnexec_wait(){
  int  i;
  int  r;
  char buff[256];
  MTNJOB *job;
  struct epoll_event ev;
  while(is_busy() && is_loop){
    r = epoll_wait(ctx->e, &ev, 1, 1000);
    if(r == 1){
      job = ev.data.ptr;
      r = read(job->pipe, buff, sizeof(buff));
      if(r == 0){
        epoll_ctl(ctx->e, EPOLL_CTL_DEL, job->pipe, NULL);
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

int mtnexec_local(STR *args, MTNJOB *job)
{
  if(!job){
    return(-1);
  }

  int p;
  int pp[2];
  int status;
  pid_t pid;
  pipe(pp);
  pid = fork();
  if(pid == -1){
    close(pp[0]);
    close(pp[1]);
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    return(-1);
  }
  if(pid){
    close(pp[1]);
    job->pid  = pid;
    job->sock = 0;
    job->pipe = pp[0];
    job->svr  = NULL;
    return(0);
  }
  close(pp[0]);
  p = pp[1];
  pid = fork();
  if(pid == -1){
    mtnlogger(mtn, 0, "[error] %s: %s\n", __func__, strerror(errno));
    close(p);
    _exit(0);
  }
  if(pid){
    waitpid(pid, &status, 0);
    close(p);
    _exit(0);
  }
  execvp(args[0], args);
  mtnlogger(mtn, 0, "[error] %s: %s %s\n", __func__, strerror(errno), args[0]);
  _exit(0); 
}

void mtnexec_remote(STR *args)
{
}

MTNJOB *mtnexec(STR *args, STR *argl)
{
  int i, j;
  int argcat = 1;
  MTNJOB *job  = NULL;
  STR *newargs = NULL;
  char buff[PATH_MAX];
  struct epoll_event ev;

  for(i=0;args[i];i++){
    newargs = realloc(newargs, sizeof(char *) * (i + 1));
    newargs[i] = newstr(args[i]);
    if(!argl){
      continue;
    } 
    if(strcmp(newargs[i], "{}") == 0){
      argcat = 0;
      newargs[i] = modstr(newargs[i], argl[0]);
    }
    if(strcmp(newargs[i], "{/}") == 0){
      argcat = 0;
      newargs[i] = modstr(newargs[i], argl[0]);
      newargs[i] = mtnexec_basename(newargs[i]);
    }
    if(strcmp(newargs[i], "{.}") == 0){
      argcat = 0;
      newargs[i] = modstr(newargs[i], argl[0]);
      newargs[i] = mtnexec_dotname(newargs[i]);
    }
    for(j=0;j<ctx->arg_num;j++){
      sprintf(buff, "{%d}", j + 1);
      if(strcmp(buff, newargs[i]) == 0){
        argcat = 0;
        newargs[i] = modstr(newargs[i], argl[j]);
        break;
      }      
    }
  }
  if(argcat && ctx->arg_num){
    newargs = realloc(newargs, sizeof(char *) * (i + ctx->arg_num));
    for(j=0;j<ctx->arg_num;j++){
      newargs[i++] = newstr(argl[j]);
    }
  }
  newargs[i] = NULL;
  job = mtnexec_wait();
  if(mtnexec_local(newargs, job) != -1){
    ev.data.ptr = job;
    ev.events   = EPOLLIN;
    epoll_ctl(ctx->e, EPOLL_CTL_ADD, job->pipe, &ev);
  }
  return(job);
}

//
// すべてのジョブが終了するまで待つ
//
int mtnexec_exit()
{
  int  i;
  int  r;
  int  status;
  char buff[256];
  struct epoll_event ev;
  MTNJOB *job;
  while(is_running() && is_loop){
    r = epoll_wait(ctx->e, &ev, 1, 1000);
    if(r == 1){
      job = ev.data.ptr;
      r = read(job->pipe, buff, sizeof(buff));
      if(r == 0){
        jclose(job);
      }
    }
  }
  if(is_running()){
    for(i=0;i<ctx->job_max;i++){
      if(ctx->job[i].pid){
        kill(ctx->job[i].pid, SIGTERM);
        jclose(&(ctx->job[i]));
      }
    }
  }
  return(0);
}

int main(int argc, char *argv[])
{
  STR *argl = NULL;
  STR *args = NULL;
  set_signal_handler();
  ctx = mtnexec_init();
  mtn = mtn_init(MODULE_NAME);
  mtn->logmode = MTNLOG_STDERR;
  args = parse(argc, argv);
  ctx->e   = epoll_create(ctx->job_max);
  ctx->job = calloc(ctx->job_max, sizeof(MTNJOB));
  if(ctx->arg_num){
    while((argl = readarg()) && is_loop){
      mtnexec(args, argl);
    }
  }else{
    mtnexec(args, NULL);
  } 
  return(mtnexec_exit());
}

