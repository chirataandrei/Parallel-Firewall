// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	ring->cap = cap;
	ring->len = 0;
	ring->read_pos = 0;
	ring->write_pos = 0;
	ring->stopped = 0;
	ring->data = malloc(cap);
	if (ring->data == NULL) {
		return -1;
	}

	pthread_mutex_init(&ring->mutex, NULL);
	pthread_cond_init(&ring->not_full, NULL);
	pthread_cond_init(&ring->not_empty, NULL);

	return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	pthread_mutex_lock(&ring->mutex);
	
	while (ring->cap - ring->len < size) {
		pthread_cond_wait(&ring->not_full, &ring->mutex);
	}
	
	if (ring->cap - ring->write_pos >= size) {
		memcpy(&ring->data[ring->write_pos], data, size);
		ring->write_pos = (ring->write_pos + size) % ring->cap;
	} else {
		size_t bytes_left = size - (ring->cap - ring->write_pos);
		memcpy(&ring->data[ring->write_pos], data, ring->cap - ring->write_pos);
		memcpy(&ring->data[0], (char *) data + ring->cap - ring->write_pos, bytes_left);
		ring->write_pos = bytes_left;
	}
	ring->len += size;

	pthread_cond_signal(&ring->not_empty);
	pthread_mutex_unlock(&ring->mutex);

	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	pthread_mutex_lock(&ring->mutex);

	while (ring->len < size && ring->stopped == 0) {
		pthread_cond_wait(&ring->not_empty, &ring->mutex);
	}

	if (ring->len < size && ring->stopped == 1) {
		pthread_mutex_unlock(&ring->mutex);
		return 0;
	}

	if (ring->cap - ring->read_pos >= size) {
		memcpy(data, &ring->data[ring->read_pos], size);
		ring->read_pos = (ring->read_pos + size) % ring->cap;
	} else {
		size_t bytes_left = size - (ring->cap - ring->read_pos);
		memcpy(data, &ring->data[ring->read_pos], ring->cap - ring->read_pos);
		memcpy((char*) data + (ring->cap - ring->read_pos), &ring->data[0], bytes_left);
		ring->read_pos = bytes_left;
	}
	ring->len -= size;

	pthread_cond_signal(&ring->not_full);
	pthread_mutex_unlock(&ring->mutex);

	return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	pthread_mutex_destroy(&ring->mutex);
	pthread_cond_destroy(&ring->not_empty);
	pthread_cond_destroy(&ring->not_full);
	free(ring->data);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	pthread_mutex_lock(&ring->mutex);
	ring->stopped = 1;
	pthread_cond_broadcast(&ring->not_empty);
	pthread_mutex_unlock(&ring->mutex);
}
