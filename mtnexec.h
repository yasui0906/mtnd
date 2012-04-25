/*
 * mtnexec.h
 * Copyright (C) 2011 KLab Inc.
 */
#define MTNEXECMODE_LOCAL  0
#define MTNEXECMODE_HYBRID 1
#define MTNEXECMODE_REMOTE 2
#define MTNEXECMODE_ALL0   3
#define MTNEXECMODE_ALL1   4
#define MTNEXECMODE_TARGET 5

#define TVMSEC(tv) (tv.tv_sec * 1000 + tv.tv_usec / 1000)

typedef struct mtnexec_context
{
  int afd;
  int efd;
  int conv;
  int mode;
  int info;
  int text;
  int opt_R;
  int opt_A;
  int opt_L;
  int nobuf;
  int child;
  STR delim;
  STR echo;
  STR group;
  STR stdin;
  STR stdout;
  STR stderr;
  ARG getarg;
  ARG putarg;
  int dryrun;
  int signal;
  int verbose;
  int arg_num;
  int job_max;
  int cpu_lim;
  int cpu_num;
  int cpu_use;
  ARG cmdargs;
  ARG linearg;
  ARG targets;
  int fsig[2];
  MTNJOB *job;
  struct timeval polltv;
} CTX;
extern int optind;

