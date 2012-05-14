#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>
#include <netdb.h>

//#define NL_MAX 3  /*Maximum number of liers*/
#define MAX_PATH 256
#define MIN(a, b)  ((a) < (b) ? (a) :(b))

int parse(char *cmd, char *operation, char *key);
int version_comparator(const void *arg1, const void *arg2);
int count_comparator(const void *arg1, const void *arg2);

int NL_MAX;

struct server_info {
	int id;
	char* hostname;
	int port;
	FILE* fp;
	int has_voted;
	int version;
	char* key;
	char* value;
	struct server_info* next;
};

typedef struct server_info server_info_t;

struct version_list {
	int version;
	int count;
	char* value;
	server_info_t* head;
};

/*This method converts host name to ip address
 *
 * REFERENCE: http://www.binarytides.com/blog/get-ip-address-from-hostname-in-c-using-linux-sockets
 */
int getipaddress(char* hostname, char* ip) {
	struct addrinfo hints, *servinfo, *p;
	struct sockaddr_in *h;
	int rv;

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC; // use AF_INET6 to force IPv6
	hints.ai_socktype = SOCK_STREAM;

	if ((rv = getaddrinfo(hostname, "http", &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return 1;
	}
	// loop through all the results and connect to the first we can
	for (p = servinfo; p != NULL; p = p->ai_next) {
		h = (struct sockaddr_in *) p->ai_addr;
		strcpy(ip, inet_ntoa(h->sin_addr));
	}

	freeaddrinfo(servinfo); // all done with this structure
	return 0;
}

int main(int argc, char* argv[]) {

	int n, nr, nw;
	char* key, *value, *operation;

	/*Check arguments*/
	char* usage = "Usage ./client <N> <NR> <NW> PUT/GET <key> {<value>}";
	if (argc < 6) {
		fprintf(stderr, "%s", usage);
		exit(0);
	} else {
		if (!strcmp(argv[4], "PUT") && argc > 7) {
			fprintf(stderr, "%s", usage);
			exit(0);
		} else if (!strcmp(argv[4], "GET") && argc > 6) {
			fprintf(stderr, "%s", usage);
			exit(0);
		}
	}

	/*store arguments*/
	n = atoi(argv[1]);
	nr = atoi(argv[2]);
	nw = atoi(argv[3]);
	operation = argv[4];
	key = argv[5];
	if (argc == 7) {
		value = argv[6];
	}

	fprintf(stderr, "\n");
	/*calculate NL_MAX*/
	if (!strcmp(operation, "GET")) {
		if (nr + nw > n) {
			int nl_max_1 = (nr + nw - n) / 2;
			int nl_max_2 = nr / 2;
			NL_MAX = MIN(nl_max_1, nl_max_2);
			fprintf(stderr, "Atmost %d liers can be identified\n", NL_MAX);
		} else {
			fprintf(stderr, "Reading quorum condition is not met\n");
			return 0;
		}
	} else if (!strcmp(operation, "PUT")) {
		if (nr + nw > n && nw > n / 2) {
			int nl_max_1 = (nr + nw - n) / 2;
			int nl_max_2 = (2 * nw - n) / 2;
			NL_MAX = MIN(nl_max_1, nl_max_2);
			fprintf(stderr, "Atmost %d liers can be identified\n", NL_MAX);
		} else {
			fprintf(stderr, "Writing quorum condition is not met\n");
			return 0;
		}
	}
	fprintf(stderr, "\n");

	/*if (!strcmp(operation, "GET")) {
	 if (!((nr + nw > n + 2 * NL_MAX) && nr > 2 * NL_MAX)) {
	 fprintf(stderr, "GET NR NW conditions are not met\n");
	 return 0;
	 }
	 } else if (!strcmp(operation, "PUT")) {
	 if (!((nr + nw > n + 2 * NL_MAX) && nw > (2 * NL_MAX + n) / 2)) {
	 fprintf(stderr, "PUT NR NW conditions are not met\n");
	 return 0;
	 }
	 }*/

	/*Read server connection details from file*/
	FILE* fp_servers = fopen("server_loc.txt", "r");
	if (!fp_servers) {
		perror("Failed to open server_loc.txt file");
		return 0;
	}
	server_info_t* slist = NULL;

	int count = 0;
	char buff[MAX_PATH];

	while (fgets(buff, MAX_PATH, fp_servers) != NULL) {
		char hostname[MAX_PATH], port[MAX_PATH];
		parse(buff, hostname, port);
		char ip[MAX_PATH];

		if (getipaddress(hostname, ip)) {
			fprintf(stderr, "Failed to convert hostname to ipaddress");
			return -1;
		}

		/*check if the server is up or not and if not store it in */
		int connfd;
		struct sockaddr_in servaddr;
		servaddr.sin_family = AF_INET;
		if (!inet_pton(AF_INET, ip, &(servaddr.sin_addr.s_addr))) {
			perror("Failed to convert address");
			exit(0);
		}
		servaddr.sin_port = htons(atoi(port));
		connfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (connect(connfd, (struct sockaddr*) &servaddr, sizeof(servaddr))) {
			fprintf(stderr, "Invalid entry in file \"%s\" : ", buff);
			perror("");
			continue;
		}
		FILE* fp = fdopen(connfd, "r+");
		if (fgetc(fp) == 'v') {
			/*allocate memory to socket address list*/
			count++;
			slist = realloc(slist, sizeof(server_info_t) * count);
			if (!slist) {
				perror("failed to read server addresses from file");
				return 0;
			}
			//store id
			slist[count - 1].id = count - 1;
			//slist[count - 1].has_voted = 1;
			slist[count - 1].hostname = strdup(hostname);
			slist[count - 1].port = atoi(port);
			slist[count - 1].fp = fp;
		} else {
			fclose(fp);
		}
	}

	int i = 0;
	if (!strcmp(operation, "PUT")) {
		// send put command to each of the server
		for (i = 0; i < count; i++) {
			fprintf(slist[i].fp, "%s %s %s\n", operation, key, value);
			fflush(NULL);
		}

		//check if votes are received
		i = 0;
		int vote_count = 0;
		for (i = 0; i < count; i++) {
			//Read a character from server if it is v then its a vote other
			if (fgetc(slist[i].fp) == 'v') {
				//that means server has voted
				slist[i].has_voted = 1;
				fprintf(stderr, "vote received from %s %d\n", slist[i].hostname,
						slist[i].port);
				vote_count++;
			}
		}
		fprintf(stderr, "\n");

		//check PUT condition
		if (vote_count < nw) {
			fprintf(stderr,
					"PUT condition not met: number of votes received: %d and NW:%d",
					vote_count, nw);
			return 0;
		}
		fprintf(stderr, "\n");

		//get version number for hosts that have voted
		for (i = 0; i < count; i++) {
			//Read a character from server if it is v then its a vote other
			if (slist[i].has_voted == 1) {
				// ask for version number
				fprintf(slist[i].fp, "a\n");
				fflush(NULL);
				//read version number
				fscanf(slist[i].fp, "%d", &(slist[i].version));
				assert(fgetc(slist[i].fp) == '\n');
				fprintf(stderr, "version no: %d at %s %d\n", slist[i].version,
						slist[i].hostname, slist[i].port);
				fflush(NULL);
			}
		}
		fprintf(stderr, "\n");
		//calculate the correct version no and also liers

		struct version_list* vlist = NULL;
		int vcount = 0;
		/*Sort slist based on version number in descending order*/
		//void qsort ( void * base, size_t num, size_t size, int ( * comparator ) ( const void *, const void * ) );
		qsort(slist, count, sizeof(server_info_t), version_comparator);

		//work on top 2NLmax+1 elements
		for (i = 0; i < (2 * NL_MAX + 1); i++) {
			//check if each element is present in version list
			int j;
			for (j = 0; j < vcount; j++) {
				if (vlist[j].version == slist[i].version) {
					//increment the count and add that element to the list
					vlist[j].count++;
					//add that element to end of the list
					server_info_t* curr = vlist[j].head;
					//insert it at the head
					if (curr == NULL) {
						vlist[j].head = &slist[i];
						slist[i].next = NULL;
					} else {
						//insert it at the tail
						while (curr->next != NULL) {
							curr = curr->next;
						}
						curr->next = &slist[i];
						slist[i].next = NULL;
					}
					break;
				}
			}
			//check if that version element is present
			if (j == vcount) {
				vcount++;
				//allocate memory for a vnode and add the current entry to it
				vlist = realloc(vlist, sizeof(struct version_list) * vcount);
				vlist[vcount - 1].version = slist[i].version;
				vlist[vcount - 1].count = 1;
				vlist[vcount - 1].head = &slist[i];
				slist[i].next = NULL;
			}
		}
		//sort vlist
		qsort(vlist, vcount, sizeof(struct version_list), count_comparator);
		int correct_vno;
		correct_vno = vlist[0].version;

		//identify liers
		for (i = 1; i < vcount; i++) {
			server_info_t* head = vlist[i].head;
			while (head != NULL) {
				fprintf(stderr, "%s %d lied with version no:%d\n",
						head->hostname, head->port, head->version);
				fflush(NULL);
				head = head->next;
			}
		}

		fprintf(stderr, "\n");

		//update each voted server with key:value:correct_vno
		for (i = 0; i < count; i++) {
			if (slist[i].has_voted) {
				fprintf(slist[i].fp, "%s:%s:%d\n", key, value, correct_vno + 1);
				fprintf(stderr,
						"Updating: Key %s with Version %d Value %s at %s %d\n",
						key, correct_vno + 1, value, slist[i].hostname,
						slist[i].port);
				fflush(NULL);
			}
		}
		fprintf(stderr, "\n");
		//send commit operation to all voted servers
		for (i = 0; i < count; i++) {
			if (slist[i].has_voted) {
				fprintf(slist[i].fp, "a\n");
			}
		}

	} else if (!strcmp(operation, "GET")) {
		// send GET command to each of the server
		for (i = 0; i < count; i++) {
			fprintf(slist[i].fp, "%s %s\n", operation, key);
			fflush(NULL);
		}

		//check if votes are received
		i = 0;
		int vote_count = 0;
		for (i = 0; i < count; i++) {
			//Read a character from server if it is v then its a vote other
			int ch = fgetc(slist[i].fp);
			fflush(NULL);
			if (ch == 'v') {
				//that means server has voted
				slist[i].has_voted = 1;
				fprintf(stderr, "vote received from %s %d\n", slist[i].hostname,
						slist[i].port);
				vote_count++;
			}
		}
		//check GET condition
		if (vote_count < nr) {
			fprintf(stderr, "\n");
			fprintf(stderr,
					"GET condition not met: number of votes received: %d and NR:%d",
					vote_count, nr);
			return 0;
		}

		fprintf(stderr, "\n");

		char buff[MAX_PATH];
		//get version number for hosts that have voted
		for (i = 0; i < count; i++) {
			//Read a character from server if it is v then its a vote other
			if (slist[i].has_voted == 1) {
				// ask for version number
				fprintf(slist[i].fp, "a\n");
				fflush(NULL);
				//read version number
				fscanf(slist[i].fp, "%d", &(slist[i].version));
				assert(fgetc(slist[i].fp) == '\n');
				if ((slist[i].version)) {
					fscanf(slist[i].fp, "%s", buff);
					slist[i].key = strdup(buff);
					fscanf(slist[i].fp, "%s", buff);
					slist[i].value = strdup(buff);
				} else {
					fscanf(slist[i].fp, "%s", buff);
					slist[i].key = strdup("");
					fscanf(slist[i].fp, "%s", buff);
					slist[i].value = strdup("");
				}
				fprintf(stderr, "version no: %d at %s %d\n", slist[i].version,
						slist[i].hostname, slist[i].port);
				fflush(NULL);
			}
		}

		fprintf(stderr, "\n");

		//calculate correct value and identify liers
		struct version_list* vlist = NULL;
		int vcount = 0;
		/*Sort slist based on version number in descending order*/
		//void qsort ( void * base, size_t num, size_t size, int ( * comparator ) ( const void *, const void * ) );
		qsort(slist, count, sizeof(server_info_t), version_comparator);

		//work on top 2NLmax+1 elements
		for (i = 0; i < n && (slist[0].version == slist[i].version); i++) {
			//check if each element is present in version list
			int j;
			for (j = 0; j < vcount; j++) {
				if (strcmp(vlist[j].value, slist[i].value) == 0) {
					//increment the count and add that element to the list
					vlist[j].count++;
					//add that element to end of the list
					server_info_t* curr = vlist[j].head;
					//insert it at the head
					if (curr == NULL) {
						vlist[j].head = &slist[i];
						slist[i].next = NULL;
					} else {
						//insert it at the tail
						while (curr->next != NULL) {
							curr = curr->next;
						}
						curr->next = &slist[i];
						slist[i].next = NULL;
					}
					break;
				}
			}
			//check if that version element is present
			if (j == vcount) {
				vcount++;
				//allocate memory for a vnode and add the current entry to it
				vlist = realloc(vlist, sizeof(struct version_list) * vcount);
				vlist[vcount - 1].version = slist[i].version;
				if (slist[i].version) {
					vlist[vcount - 1].value = strdup(slist[i].value);
				} else {
					vlist[vcount - 1].value = "";
				}
				vlist[vcount - 1].count = 1;
				vlist[vcount - 1].head = &slist[i];
				slist[i].next = NULL;
			}
		}
		//sort vlist
		qsort(vlist, vcount, sizeof(struct version_list), count_comparator);
		int correct_vno;
		correct_vno = vlist[0].version;

		//identify liers
		int nr_liers = 0;
		for (i = 1; i < vcount && nr_liers < NL_MAX; i++) {
			server_info_t* head = vlist[i].head;
			while (head != NULL && nr_liers < NL_MAX) {
				fprintf(stderr, "%s %d lied with version no:%d\n",
						head->hostname, head->port, head->version);
				fflush(NULL);
				head = head->next;
				nr_liers++;
			}
		}
		fprintf(stderr, "\n");

		//print correct version numbers
		server_info_t* head = vlist[0].head;
		while (head != NULL) {
			fprintf(stderr, "highest version at %s %d with version no:%d\n",
					head->hostname, head->port, head->version);
			fflush(NULL);
			head = head->next;
		}
		fprintf(stderr, "\n");

		if (correct_vno) {
			fprintf(stderr, "Reading key/value :%s/%s\n", key, vlist[0].value);
		} else {
			fprintf(stderr, "No key value pair exists for key %s\n", key);
		}

		fprintf(stderr, "\n");

	} else {
		fprintf(stderr, "invalid command");
	}

	return 0;
}

int version_comparator(const void *arg1, const void *arg2) {
	server_info_t* s1 = (server_info_t*) arg1;
	server_info_t* s2 = (server_info_t*) arg2;

	if (s1->has_voted != 1) {
		s1->version = -1;
	}

	return !(s1->version > s2->version);
}

int count_comparator(const void *arg1, const void *arg2) {
	struct version_list* s1 = (struct version_list*) arg1;
	struct version_list* s2 = (struct version_list*) arg2;

	return !(s1->count > s2->count);
}

/*Parses the command sent from the client*/
int parse(char *cmd, char *operation, char *key) {
	int i = 0;
	while (cmd[i] != '\0' && isspace(cmd[i++]))
		;

	i--;
	if (cmd[i] == '\0') {
		return -1;
	}

	int j = 0;
	while (cmd[i] != '\0' && !isspace(cmd[i]) && i < MAX_PATH && j < MAX_PATH) {
		operation[j++] = cmd[i++];
	}
	operation[j] = '\0';

	if (cmd[i] == '\0') {
		return -1;
	}

	while (cmd[i] != '\0' && isspace(cmd[i++]))
		;

	i--;
	if (cmd[i] == '\0') {
		return -1;
	}

	j = 0;
	while (cmd[i] != '\0' && !isspace(cmd[i]) && i < MAX_PATH && j < MAX_PATH) {
		key[j++] = cmd[i++];
	}
	key[j] = '\0';
	return 0;
}

