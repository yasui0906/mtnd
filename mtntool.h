/*
 * mtntool.h
 * Copyright (C) 2011 KLab Inc.
 */
#define MTNTOOL_ERROR   0
#define MTNTOOL_CONSOLE 1
#define MTNTOOL_INFO    2
#define MTNTOOL_LIST    3
#define MTNTOOL_PUT     4
#define MTNTOOL_GET     5
#define MTNTOOL_DEL     6
#define MAX(a, b) ((a < b) ? b : a)

typedef struct mtntool_context
{
  int mode;
  STR local_path;
  STR remote_path;
} CTX;
