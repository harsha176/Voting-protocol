#ifndef TABLE_H
#define TABLE_H

/*This defines value*/
struct __value_t {
   char* act_val;
   int version;
};

typedef struct __value_t value_t;

typedef struct __entry_t entry_t;

/*Each entry is a linked list*/
struct __entry_t {
    char *key;
    value_t* value;
    int lock;
    entry_t *next;
};

/*This method fetches the value of key from table*/
entry_t* get(char* key);

/*This method updates the key with given value*/
void put(char* key, value_t* table);

#endif
