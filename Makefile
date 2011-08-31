all: mtnfs.c mtntool.c mtnfs.h libmtnfs fuse
	gcc -DMODULE_NAME=\"mtntool\" -o mtntool mtntool.c common.c libmtnfs.a
	gcc -DMODULE_NAME=\"mtnfs\"   -o mtnfs   mtnfs.c   common.c libmtnfs.a

fuse: mtnmount.c mtnfs.h 
	gcc -DMODULE_NAME=\"mtnmount\" -lfuse -o mtnmount mtnmount.c common.c libmtnfs.a -I/usr/klab/app/fuse/include -L/usr/klab/app/fuse/lib

libmtnfs: libmtnfs.c mtnfs.h
	gcc -c libmtnfs.c
	ar r libmtnfs.a libmtnfs.o

clean:
	rm -f mtnfs mtntool mtnmount libmtnfs.a *.o

sync:
	rsync -a *.c *.h adc:LOCAL/mtnfs/
	ssh adc "cd LOCAL/mtnfs/;make"

install:
	rsync -a mtnfs    /usr/klab/sbin/mtnfs
	rsync -a mtntool  /usr/klab/sbin/mtntool
	rsync -a mtnmount /usr/klab/sbin/mtnmount
