/*

*/
#include "mtnfs.h"
#include <fuse.h>

static int mtn_getattr(const char *path, struct stat *stbuf)
{
  int res = 0;
  memset(stbuf, 0, sizeof(struct stat));
  stbuf->st_mode  = S_IFREG | 0644;
  stbuf->st_nlink = 1;
  stbuf->st_size  = strlen(0);
  //res = -ENOENT;
  return res;
}

static int mtn_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
  /*
  (void) offset;
  (void) fi;
  if(strcmp(path, "/") != 0){
	return -ENOENT;
  }
  filler(buf, ".",  NULL, 0);
  filler(buf, "..", NULL, 0);
  //filler(buf, hello_path + 1, NULL, 0);
  */
  return 0;
}

static int mtn_open(const char *path, struct fuse_file_info *fi)
{
/*
    if (strcmp(path, hello_path) != 0)
        return -ENOENT;
    if ((fi->flags & 3) != O_RDONLY)
        return -EACCES;
*/
    return 0;
}

static int mtn_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
/*
    size_t len;
    (void) fi;
    if(strcmp(path, hello_path) != 0)
        return -ENOENT;

    len = strlen(hello_str);
    if (offset < len) {
        if (offset + size > len)
            size = len - offset;
        memcpy(buf, hello_str + offset, size);
    } else
        size = 0;
*/
  return size;
}

static int mtn_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  return size;
}

static struct fuse_operations mtn_oper = {
  .getattr    = mtn_getattr,
  .readdir    = mtn_readdir,
  .open       = mtn_open,
  .read       = mtn_read,
};

int main(int argc, char *argv[])
{
  return fuse_main(argc, argv, &mtn_oper, NULL);
}

