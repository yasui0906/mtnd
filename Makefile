all: mtnfs.c mtntool.c mtnfs.h
	gcc -DMODULE_NAME=\"mtntool\" -o mtntool mtntool.c common.c
	gcc -DMODULE_NAME=\"mtnfs\"   -o mtnfs   mtnfs.c   common.c

fuse: mtnmount.c common.c mtnfs.h
	gcc -DMODULE_NAME=\"mtnmount\" -lfuse -o mtnmount mtnmount.c common.c

clean:
	rm -f mtnfs mtntool mtnmount *.o

sync:
	rsync -a *.c *.h Makefile adc:LOCAL/mtnfs/
	ssh adc "cd LOCAL/mtnfs/;make"

install:
	rsync -a mtnfs   /usr/klab/sbin/mtnfs
	rsync -a mtntool /usr/klab/sbin/mtntool
