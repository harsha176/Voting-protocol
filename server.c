#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/time.h>
#include <netdb.h>

#include "table.h"

#define MAX_PATH 256
#define MAX_LISTEN_QUEUE 10
#define WAIT_TIME 60

/*Arguments to thread function*/
struct thread_arg {
    int connfd;			// connection socket handle
    struct sockaddr_in clientaddr;	// client address
};

int is_lier;			/*Sets to true if configured as lier */

typedef enum {
    PUT, GET
} operation_t;

float reply_prob;		/*Reply probability */

/*Perform clean up operation*/
void clean_up(entry_t * /*entry */ , int /*connfd */ );

/*Handle each client in a seperate thread*/
void *handle_client(void *arg);

/*Parses the command sent from the client*/
int parse_command(char *cmd, char *operation, char *key, char *value);

void parse_put_cmd(char *buff, char *new_key, char *new_act_value,
		   int *new_version);

int main(int argc, char *argv[])
{
    int port;			/*Listening port number */

    /*Check arguments */
    if (argc != 4) {
	printf("Usage ./server <port> <prob of replying> <lier> \n");
	return 0;
    }

    /*Parse arguments and store them in appropriate variables */
    reply_prob = atof(argv[2]);
    port = atoi(argv[1]);
    is_lier = atoi(argv[3]) > 0 ? true : false;

    /*Write hostname and port to server_loc.txt file*/
    char hostname[MAX_PATH];
    if(gethostname(hostname, sizeof(char)*MAX_PATH)) {
	    perror("Failed to get hostname of the machine");
	    return -1;
    }
    FILE* fsl = fopen("server_loc.txt", "a");
    fprintf(fsl, "%s %d\n", hostname, port);
    fclose(fsl);

    /*Initialize socket data structures and configure it as server socket */
    struct sockaddr_in servaddr;
    int listenfd;
    listenfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (!listenfd) {
	perror("Failed to get listening socket handle");
	exit(1);
    }
    //Initialize server address
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    // bind socket to above address  
    if (bind(listenfd, (struct sockaddr *) &servaddr, sizeof(servaddr))) {
	perror("Failed to bind to 9000 port");
	exit(1);
    }
    // set socket state to listening
    if (listen(listenfd, MAX_LISTEN_QUEUE)) {
	perror("Failed to tag the given socket as listening");
	exit(2);
    }


    int connfd;			// connected socket handle
    struct sockaddr_in clientaddr;	// Client address
    int client_addrlen;

    /*Waiting for clients to connect */
    while (1) {
	/*Wait till a client accepts connection */
	connfd =
	    accept(listenfd, (struct sockaddr *) &clientaddr,
		   &client_addrlen);
	if (!connfd) {
	    perror("Failed to establish connection with client");
	    exit(2);
	};

	//launch client thread with connection params
	struct thread_arg *conn_params =
	    (struct thread_arg *) malloc(sizeof(struct thread_arg));
	if (!conn_params) {
	    perror("Failed to launch thread for client");
	    exit(5);
	}

	conn_params->connfd = connfd;
	conn_params->clientaddr = clientaddr;

	pthread_t client_tid;
	pthread_create(&client_tid, NULL, handle_client, conn_params);
    }

    close(listenfd);
    return 0;
}


/*Handle each client in a seperate thread*/
void *handle_client(void *arg)
{
    assert(arg);
    struct thread_arg *conn_params = (struct thread_arg *) arg;
    int connfd = conn_params->connfd;
    struct sockaddr_in clientaddr = conn_params->clientaddr;
    free(conn_params);

    char client_hostname[INET_ADDRSTRLEN];
    int client_port;

    client_port = clientaddr.sin_port;

    char client_ipaddress[INET_ADDRSTRLEN];
    //access client address details
    if (!inet_ntop
	(AF_INET, &(clientaddr.sin_addr), client_ipaddress,
	 INET_ADDRSTRLEN)) {
	perror("Failed to retrieve client address");
	pthread_exit(0);
    }
    // get hostname of the client

    if(!getnameinfo((struct sockaddr*)&clientaddr, sizeof(clientaddr),
		                    client_hostname, INET_ADDRSTRLEN,
				                    NULL, 0, NI_NOFQDN)) {
	    perror("Failed to get hostname of the client");
	    return NULL;
    }

    FILE *fp = fdopen(connfd, "r+");
    //Simulate server failure
    double r = (rand() + 0.0) / RAND_MAX;
    // If generated random number is greater than given probability then don't vote.
    // fprintf(stderr, "random number is : %f and reply prob is %f\n", r, reply_prob);
    if (r > reply_prob) {
	fprintf(fp, "u");
	fflush(NULL);
	close(connfd);
	pthread_exit(0);
    }
    fprintf(fp, "v");
    fflush(NULL);
    // perform voting and communicate with the client
    char buff[MAX_PATH];	// temp buffer
    int len;			// buffer length
    if (!fp) {
	perror("Failed to get handle of connected socket");
	exit(4);
    }

		/**
		 * Protocol between client and server
		 * 1. Client will send <OPERATION> <KEY> {<VALUE>}
		 * 2. Server performs following actions based on the operation
		 * 	a) PUT 
		 * 		1. if key doesn't exist vote back with version number 0
		 * 		2. else it locks the enrty, votes back and returns entry with version number
		 * 		   ( if is_lie is true then send a false version number or entry)
		 * 		3. Client sends updated key value pair and version # to server
		 * 		4. Server updates entry and version #
		 * 		5. Wait for client to send all servers updated message
		 * 		6. if client fails to send a message within a given time it releases lock, 
		 * 		   retains previous value and closes connection
		 *
		 *	b) GET
		 *		1. if key doesn't exist vote back with version number 0	
		 *		2. else it votes back and returns entry with version number
		 *		( if is_lie is true then send a false version number or entry)
		 *	
		 * 3. Closes conncetion with client
		 *
		 */
    fgets(buff, MAX_PATH, fp);
    len = strlen(buff);
    buff[len - 1] = '\0';
    char operation[MAX_PATH], key[MAX_PATH], value[MAX_PATH];
    if (parse_command(buff, operation, key, value)) {
	fprintf(fp, "Invalid command %s\n", buff);
	close(connfd);
	return 0;
    }

    /*print status to console */
    fprintf(stderr, "contacted by %s %d for %s %s\n",
	    client_hostname, client_port, operation, key);
    if (!strcmp(operation, "GET")) {
	//TODO for GET operation
	entry_t *entry = get(key);
	// check if lock exists if so do not vote
	if (entry != NULL && entry->lock) {
	    fprintf(fp, "l");
	    fflush(NULL);
	    close(connfd);
	    pthread_exit(0);
	}
	fprintf(fp, "v");
	fflush(NULL);
	char dummy_buff[MAX_PATH];
	fgets(dummy_buff, MAX_PATH, fp);
	if (entry) {
	    int version_no = entry->value->version;
	    char* value = strdup(entry->value->act_val);
	    fprintf(stderr, "sending version No %d to %s %d\n",
		    version_no, client_hostname, client_port);
	    fprintf(fp, "%d\n", version_no);
	    if (is_lier) {
		value[0] += 1;
		fprintf(stderr, "lied to %s %d key: %s with value: %s\n",
			client_hostname, client_port, key, value);
	    }
	    fprintf(fp, "%s\n%s\n", entry->key, value);
	    fprintf(stderr, "sending key %s, value %s to %s %d\n",
		    entry->key, value, client_hostname, client_port);

	} else {

	    fprintf(fp, "0\n");
	    fprintf(stderr, "sending version No 0 to %s %d\n",
		    client_hostname, client_port);
	    fprintf(stderr, "lied to %s %d key: %s with value: %s\n",
			client_hostname, client_port, key, value);

	}
	fflush(NULL);
    } else {
	// TODO for PUT operation
	//fetch the key for that value in the table
	entry_t *entry = get(key);
	// check if lock exists if so do not vote
	if (entry != NULL && entry->lock) {
	    fprintf(fp, "l");
	    fflush(NULL);
	    close(connfd);
	    pthread_exit(0);
	}
	// if version number is not zero lock and then vote
	if (entry) {
	    entry->lock = 1;
	    fprintf(stderr, "lock on %s\n", key);
	}
	// send vote
	fprintf(fp, "v");
	fflush(NULL);
	int k = 0;

	fd_set rfds, wfds, efds;
	int status;
	struct timeval tm;
	FD_ZERO(&rfds);
	FD_ZERO(&wfds);
	FD_ZERO(&efds);

	FD_SET(connfd, &rfds);

	tm.tv_sec = WAIT_TIME;
	tm.tv_usec = 0;
	// set timer and wait for response from client
	status = select(connfd + 1, &rfds, &wfds, &efds, &tm);
	if (!status) {
	    clean_up(entry, connfd);
	}

	char dummy[MAX_PATH];
	fgets(dummy, MAX_PATH, fp);

	// do not lock return version number and details
	if (!entry) {
	    if (is_lier) {
		fprintf(fp, "1000000\n");
		fprintf(stderr, "sending version No 1000000 to %s %d\n",
			client_hostname, client_port);
		fprintf(stderr, "lied to %s %d %s\n", client_hostname,
			client_port, key);
	    } else {
		fprintf(fp, "0\n");
		fprintf(stderr, "sending version No 0 to %s %d\n",
			client_hostname, client_port);
	    }

	} else {
	    int version_no = entry->value->version;
	    if (is_lier) {
		version_no += 1000000;
		fprintf(stderr, "lied to %s %d %s\n", client_hostname,
			client_port, key);
	    }
	    fprintf(fp, "%d\n", version_no);
	    fprintf(stderr, "sending version No %d to %s %d\n",
		    version_no, client_hostname, client_port);
	}
	fflush(NULL);
	// read data from client block for certain time  
	status = select(connfd + 1, &rfds, &wfds, &efds, &tm);

	//reached time out
	if (!status) {
	    clean_up(entry, connfd);
	}


	/*else {
	   int version_no = entry->value->version;
	   if (is_lier) {
	   version_no += 1000000;
	   fprintf(stderr, "lied to %s %d %s\n", client_hostname,
	   client_port, key);
	   }
	   fprintf(fp, "%d\n", version_no);
	   fprintf(stderr, "sending version No %d to %s %d\n",
	   version_no, client_hostname, client_port);
	   } */
	fflush(NULL);
	// read data from client block for certain time  
	status = select(connfd + 1, &rfds, &wfds, &efds, &tm);

	//reached time out
	if (!status) {
	    clean_up(entry, connfd);
	}
	//get key value and version number and update the entry 
	char new_key[MAX_PATH];
	char new_act_value[MAX_PATH];
	int new_version;
	value_t *old_value;

	if (entry) {
	    old_value = entry->value;
	}

	fgets(buff, MAX_PATH, fp);
	parse_put_cmd(buff, new_key, new_act_value, &new_version);
	assert(!strcmp(new_key, key));

	value_t *new_value = (value_t *) malloc(sizeof(value_t));
	new_value->act_val = strdup(new_act_value);
	new_value->version = new_version;
	fprintf(stderr,
		"Writing key:%s value:%s with updated version no %d\n",
		key, new_value->act_val, new_value->version);
	fflush(NULL);
	put(key, new_value);
	entry_t *new_entry;
	// lock new enrtry
	if (!entry) {
	    //get 
	    new_entry = get(key);
	    new_entry->lock = 1;
	}
	//wait for client to send commit signal
	// read data from client block for certain time  
	status = select(connfd + 1, &rfds, &wfds, &efds, &tm);

	//reached time out
	if (!status) {
	    if (entry) {
		entry->value = old_value;
		clean_up(entry, connfd);
	    } else {
		entry->value = NULL;
		clean_up(new_entry, connfd);
	    }
	}
	//release lock 
	if (entry) {
	    entry->lock = 0;
	} else {
	    new_entry->lock = 0;
	}
    }

    //}
    fprintf(stderr, "\n");
    close(connfd);
    return NULL;
}

void clean_up(entry_t * entry, int connfd)
{

    //reached time out
    if (entry) {
	entry->lock = 0;
    }
    close(connfd);
    pthread_exit(0);
}

/*Parses the command sent from the client*/
int parse_command(char *cmd, char *operation, char *key, char *value)
{
    int i = 0;
    while (cmd[i] != '\0' && isspace(cmd[i++]));

    i--;
    if (cmd[i] == '\0') {
	return -1;
    }

    int j = 0;
    while (cmd[i] != '\0' && !isspace(cmd[i]) && i < MAX_PATH
	   && j < MAX_PATH) {
	operation[j++] = cmd[i++];
    }
    operation[j] = '\0';

    if (cmd[i] == '\0') {
	return -1;
    }

    while (cmd[i] != '\0' && isspace(cmd[i++]));

    i--;
    if (cmd[i] == '\0') {
	return -1;
    }

    j = 0;
    while (cmd[i] != '\0' && !isspace(cmd[i]) && i < MAX_PATH
	   && j < MAX_PATH) {
	key[j++] = cmd[i++];
    }
    key[j] = '\0';

    if (cmd[i] == '\0' && !strcmp(operation, "GET")) {
	return 0;
    }

    if (strcmp(operation, "PUT")) {
	return -1;
    }
    // Its a put request fetch value
    while (cmd[i] != '\0' && isspace(cmd[i++]));
    i--;

    if (cmd[i] == '\0') {
	return -1;
    }

    j = 0;
    while (cmd[i] != '\0' && !isspace(cmd[i]) && i < MAX_PATH
	   && j < MAX_PATH) {
	value[j++] = cmd[i++];
    }
    value[j] = '\0';

    return 0;
}

void parse_put_cmd(char *buff, char *new_key, char *new_act_value,
		   int *new_version)
{
    int i, j;
    i = 0;
    j = 0;
    while (buff[i] != '\0' && buff[i] != ':') {
	new_key[j++] = buff[i++];
    }
    new_key[j] = '\0';

    if (buff[i] != '\0' && buff[i] == ':') {
	i++;
    }

    j = 0;
    while (buff[i] != '\0' && buff[i] != ':') {
	new_act_value[j++] = buff[i++];
    }
    new_act_value[j] = '\0';

    if (buff[i] != '\0' && buff[i] == ':') {
	i++;
    }

    j = 0;
    char version_buff[MAX_PATH];
    while (buff[i] != '\0' && buff[i] != ':') {
	version_buff[j++] = buff[i++];
    }
    version_buff[j] = '\0';
    *new_version = atoi(version_buff);

}
