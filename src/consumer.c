// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void *consumer_thread(void *arg)
{
	so_consumer_ctx_t *ctx = arg;
	char out_buf[PKT_SZ];
	so_packet_t pkt;

	while (1) {
		unsigned long seq;
		ssize_t ret = ring_buffer_dequeue(ctx->producer_rb, &pkt,
						  sizeof(pkt), &seq);

		if (ret == 0)
			break;

		int action = process_packet(&pkt);
		unsigned long hash = packet_hash(&pkt);
		unsigned long timestamp = pkt.hdr.timestamp;
		int len = snprintf(out_buf, sizeof(out_buf), "%s %016lx %lu\n",
				   RES_TO_STR(action), hash, timestamp);

		pthread_mutex_lock(ctx->mutex);
		while (*ctx->next_seq != seq)
			pthread_cond_wait(ctx->cond, ctx->mutex);

		write(ctx->out_fd, out_buf, len);
		(*ctx->next_seq)++;
		pthread_cond_broadcast(ctx->cond);
		pthread_mutex_unlock(ctx->mutex);
	}

	return NULL;
}

int create_consumers(pthread_t *tids,
		     int num_consumers,
		     struct so_ring_buffer_t *rb,
		     const char *out_filename)
{
	int out_fd = open(out_filename, O_RDWR | O_CREAT | O_TRUNC, 0666);
	DIE(out_fd < 0, "open");

	pthread_mutex_t *out_mutex = malloc(sizeof(*out_mutex));
	pthread_cond_t *out_cond = malloc(sizeof(*out_cond));
	unsigned long *next_seq = malloc(sizeof(*next_seq));
	so_consumer_ctx_t *ctx = malloc(num_consumers * sizeof(*ctx));

	DIE(out_mutex == NULL || out_cond == NULL || next_seq == NULL ||
	    ctx == NULL, "malloc");

	pthread_mutex_init(out_mutex, NULL);
	pthread_cond_init(out_cond, NULL);
	*next_seq = 0;

	for (int i = 0; i < num_consumers; i++) {
		int rc;

		ctx[i].producer_rb = rb;
		ctx[i].mutex = out_mutex;
		ctx[i].cond = out_cond;
		ctx[i].next_seq = next_seq;
		ctx[i].out_fd = out_fd;
		rc = pthread_create(&tids[i], NULL, consumer_thread, &ctx[i]);
		DIE(rc != 0, "pthread_create");
	}

	return num_consumers;
}
