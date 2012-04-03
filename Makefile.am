if ENABLE_MTNFS
sbin_PROGRAMS = mtnd mtnfs
mtnfs_SOURCES = mtnfs.c common.c
mtnfs_OPTS    = -DMODULE_NAME=\"mtnfs\"
mtnfs_CFLAGS  = -g -Wall $(mtnfs_OPTS) $(FUSE_CFLAGS)
mtnfs_LDADD   = libmtn.a -lfuse $(FUSE_LDFLAGS)
else
sbin_PROGRAMS = mtnd
endif
bin_PROGRAMS  = mtnexec
lib_LIBRARIES = libmtn.a
libmtn_a_SOURCES = libmtn.c
libmtn_a_CFLAGS = -Wall
mtnd_SOURCES    = mtnd.c common.c
mtnexec_SOURCES = mtnexec.c common.c
mtnd_OPTS       = -DMODULE_NAME=\"mtnd\"
mtnexec_OPTS    = -DMODULE_NAME=\"mtnexec\"
mtnd_CFLAGS     = -g -Wall $(mtnd_OPTS)
mtnexec_CFLAGS  = -g -Wall $(mtnexec_OPTS)
mtnd_LDADD      = libmtn.a
mtnexec_LDADD   = libmtn.a
