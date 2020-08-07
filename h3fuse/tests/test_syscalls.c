/*
 * test_syscalls.c
 *
 *  Created on: Feb 25, 2020
 *      Author: epolitis
 */

#include <stdio.h>

int setup(){
	return 1;
}

int main(){


	if(!setup()){
		printf("Unable to setup");
		return -1;
	}


	return 0;
}

