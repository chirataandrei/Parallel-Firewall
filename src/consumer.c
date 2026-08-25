	t// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void consumer_thread(so_consumer_ctx_t *ctx)
{
	/* TODO: implement consumer thread */
	(void) ctx;
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{
	int out_fd = open(out_filename, O_RDWR|O_CREAT|O_TRUNC, 0666);
	DIE(out_fd < 0, "open");

	pthread_mutex_t out_mutex;
	pthread_mutex_init(&out_mutex, NULL);

	so_consumer_ctx_t *ctx = malloc(num_consumers * sizeof(so_consumer_ctx_t));
	DIE(ctx == NULL, "malloc");

	for (int i = 0; i < num_consumers; i++) {
		ctx[i].producer_rb = rb;
		ctx[i].mutex = out_mutex;
		pthread_create(&tids[i], NULL, (void *(*)(void *))consumer_thread, &ctx[i]);		
	}

	return num_consumers;
}
