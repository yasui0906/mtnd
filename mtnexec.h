/*
 * mtnexec.h
 * Copyright (C) 2011 KLab Inc.
 */
#define MTNEXECMODE_LOCAL  0
#define MTNEXECMODE_HYBRID 1
#define MTNEXECMODE_REMOTE 2
#define MTNEXECMODE_ALL0   3
#define MTNEXECMODE_ALL1   4

typedef struct mtnexec_context
{
  int efd;
  int zero;
  int conv;
  int mode;
  int info;
  int opt_R;
  int opt_A;
  int opt_L;
  int nobuf;
  int child;
  STR group;
  STR stdin;
  STR stdout;
  STR stderr;
  ARG getarg;
  ARG putarg;
  MTNSVR *svr;
  MTNJOB *job;
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
  int fsig[2];
  struct timeval polltv;
} CTX;
extern int optind;
