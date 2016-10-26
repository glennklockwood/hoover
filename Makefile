.PHONY: clean

RMQ_C_DIR=$(PWD)/rabbitmq-c-0.8.0/_install
OTHER_PKGS_DIR=/opt/local

CFLAGS=-I$(RMQ_C_DIR)/include -I$(OTHER_PKGS_DIR)/include -Wno-deprecated-declarations -g -std=c99
LDFLAGS=-L$(RMQ_C_DIR)/lib -L$(OTHER_PKGS_DIR)/lib -Bstatic

OBJECTS=producer producer-file test-hdo test-manifest

all: $(OBJECTS)

producer: CFLAGS += -DHOOVER_APP_ID=\"hoover-producer-cli\"
producer: producer.c hooverio.o hooverrmq.o
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDFLAGS) -lrabbitmq -lssl -lcrypto -lz 

hooverrmq.o: hooverrmq.c hooverrmq.h
	$(CC) $(CPPFLAGS) -DHOOVER_CONFIG_FILE=\"amqpcreds.conf\"  $(CFLAGS) -c $<

producer-file: producer.c hooverio.o hooverfile.o
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDFLAGS) -lssl -lcrypto -lz

hooverfile.o: hooverfile.c hooverfile.h
	$(CC) $(CPPFLAGS)  $(CFLAGS) -c $<

hooverio.o: hooverio.c hooverio.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $<

test-hdo: test-hdo.c hooverio.o hooverfile.o
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDFLAGS) -lssl -lcrypto -lz

test-manifest: test-manifest.c hooverio.o hooverfile.o
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDFLAGS) -lssl -lcrypto -lz

clean:
	-rm *.o $(OBJECTS)
	-find ./ -type d -name \*.dSYM -print0 | xargs -0 rm -r
