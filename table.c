#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include "table.h"


//table 
entry_t* head = NULL;

/*This method fetches the value of key from table*/
entry_t* get(char* key) {
      entry_t* curr = head;
      while(curr != NULL && strcmp(key, curr->key)) {
          curr = curr->next;
      }
      return curr;
}



/*This method updates the key with given value*/
void put(char* key, value_t* value){
	// check if key is present
      entry_t* curr = head;
      while(curr != NULL && strcmp(key, curr->key)) {
          curr = curr->next;
      }

      // key is present, update value
      if(curr != NULL) {
	  curr->value = value;
          return;
      }
	
      // else create a new key with this value and set version to 0
      if(head == NULL) {
	     head = (entry_t*)malloc(sizeof(entry_t)); 
	     head->key = strdup(key);
	     head->value = value;
             head->lock = 0;
             head->next = NULL;
             return ;
      }

      curr = head;
      while(curr->next != NULL) {
          curr = curr->next;
     }

     curr->next = (entry_t*)malloc(sizeof(entry_t)); 
     curr->next->key = strdup(key);
     curr->next->value = value;
}
