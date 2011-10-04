/*
 * mtnexec.h
 * Copyright (C) 2011 KLab Inc.
 */
typedef char *STR;
typedef struct mtnexec_context
{
  int e;
  char *group;
  MTNSVR *svr;
  MTNJOB *job;
  int arg_num;
  int job_max;
} CTX;
extern int optind;
