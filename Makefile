.PHONY: clean

CFLAGS=-I/opt/local/include -Wno-deprecated-declarations -g
LDFLAGS=-L/opt/local/lib -Bstatic

OBJECTS=producer test-hdo test-manifest

all: $(OBJECTS)

producer: producer.c hooverio.o hooverrmq.o
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDFLAGS) -lrabbitmq -lssl -lcrypto -lz

hooverrmq.o: hooverrmq.c hooverrmq.h
	$(CC) $(CPPFLAGS) -DHOOVER_CONFIG_FILE=\"amqpcreds.conf\"  $(CFLAGS) -c $<

hooverio.o: hooverio.c hooverio.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $<

test-hdo: test-hdo.c hooverio.o
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDFLAGS) -lssl -lcrypto -lz

test-manifest: test-manifest.c hooverio.o
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDFLAGS) -lssl -lcrypto -lz

clean:
	-rm *.o $(OBJECTS)
	-find ./ -type d -name \*.dSYM -print0 | xargs -0 rm -r
