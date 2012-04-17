if ENABLE_DEBUG
DEBUG_OPTS = -g -DMTN_DEBUG
endif
if ENABLE_MTNFS
sbin_PROGRAMS = mtnd mtnfs
mtnfs_SOURCES = mtnfs.c common.c
mtnfs_OPTS    = -DMODULE_NAME=\"mtnfs\"
mtnfs_CFLAGS  = -Wall $(DEBUG_OPTS) $(mtnfs_OPTS) $(FUSE_CFLAGS)
mtnfs_LDADD   = libmtn.a -lfuse $(FUSE_LDFLAGS)
else
sbin_PROGRAMS = mtnd
endif
bin_PROGRAMS     = mtnexec
lib_LIBRARIES    = libmtn.a
libmtn_a_SOURCES = libmtn.c
mtnd_SOURCES     = mtnd.c common.c
mtnexec_SOURCES  = mtnexec.c common.c
libmtn_a_CFLAGS  = -Wall $(DEBUG_OPTS)
mtnd_OPTS        = -DMODULE_NAME=\"mtnd\"
mtnexec_OPTS     = -DMODULE_NAME=\"mtnexec\"
mtnd_CFLAGS      = -Wall $(DEBUG_OPTS) $(mtnd_OPTS)
mtnexec_CFLAGS   = -Wall $(DEBUG_OPTS) $(mtnexec_OPTS)
mtnd_LDADD       = libmtn.a
mtnexec_LDADD    = libmtn.a
