all:server client

server:server.o table.o
	gcc -g table.o server.o -lpthread -o server
table.o:table.c table.h
	gcc -g -c table.c
server.o:server.c
	gcc -g -c server.c
client:client.c
	gcc -g client.c -o client

clean:
	rm -rf client server.o table.o server
