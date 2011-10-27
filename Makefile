all: lib mtnd mtnfs tool exec test

lib: libmtn.c libmtn.h
	gcc -c -Wall libmtn.c
	ar r libmtn.a libmtn.o

mtnd: mtnd.c lib
	gcc -DMODULE_NAME=\"mtnd\" -g -o mtnd -Wall mtnd.c common.c libmtn.a

mtnfs: mtnfs.c lib
	gcc -DMODULE_NAME=\"mtnfs\" -g -o mtnfs -Wall -l fuse mtnfs.c common.c libmtn.a -I. -I/usr/klab/app/fuse/include -L/usr/klab/app/fuse/lib

tool: mtntool.c lib
	gcc -DMODULE_NAME=\"mtntool\" -g -o mtntool -I. mtntool.c libmtn.a

exec: mtnexec.c mtnexec.h lib
	gcc -DMODULE_NAME=\"mtnexec\" -g -o mtnexec -Wall -I. mtnexec.c libmtn.a

test: mtntest.c mtnd mtnfs
	gcc -DMODULE_NAME=\"mtntest\" -o mtntest -I. mtntest.c libmtn.a

clean:
	rm -f libmtn.a mtnd mtnfs mtntool mtnexec mtntest *.o

sync:
	rsync -a *.c *.h Makefile adc:LOCAL/mtnfs/
	ssh adc "cd LOCAL/mtnfs/;make"

install:
	rsync -a mtnd    /usr/klab/sbin/mtnd
	rsync -a mtnfs   /usr/klab/sbin/mtnfs
	rsync -a mtntool /usr/klab/bin/mtntool
	rsync -a mtnexec /usr/klab/bin/mtnexec
	chown root:root  /usr/klab/sbin/mtnd
	chown root:root  /usr/klab/sbin/mtnfs
	chown root:root  /usr/klab/bin/mtntool
	chown root:root  /usr/klab/bin/mtnexec
