/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_extent.h"

extern struct workqueue_struct *extent_work_queue;

static void extent_writer_tx_work_cb(struct work_struct *work);
static void extent_writer_rx_work_cb(struct work_struct *work);

struct cfs_extent_writer *cfs_extent_writer_new(struct cfs_extent_stream *es,
						struct cfs_data_partition *dp,
						loff_t file_offset, u64 ext_id,
						u64 ext_offset, u32 ext_size)
{
	struct cfs_extent_writer *writer;
	int ret;

	BUG_ON(dp == NULL);
	writer = kzalloc(sizeof(*writer), GFP_NOFS);
	if (!writer)
		return ERR_PTR(-ENOMEM);
	if (es->enable_rdma) {
		ret = cfs_rdma_create(&dp->members.base[0], es->ec->log,
				      &writer->sock, es->rdma_port);
	} else {
		ret = cfs_socket_create(&dp->members.base[0],
					es->ec->log, &writer->sock);
	}

	if (ret < 0) {
		kfree(writer);
		return ERR_PTR(ret);
	}
	writer->es = es;
	writer->dp = dp;
	writer->file_offset = file_offset;
	writer->ext_id = ext_id;
	writer->ext_offset = ext_offset;
	writer->ext_size = ext_size;
	writer->w_size = ext_size;
	spin_lock_init(&writer->lock_tx);
	spin_lock_init(&writer->lock_rx);
	INIT_LIST_HEAD(&writer->tx_packets);
	INIT_LIST_HEAD(&writer->rx_packets);
	INIT_WORK(&writer->tx_work, extent_writer_tx_work_cb);
	INIT_WORK(&writer->rx_work, extent_writer_rx_work_cb);
	init_waitqueue_head(&writer->tx_wq);
	init_waitqueue_head(&writer->rx_wq);
	atomic_set(&writer->tx_inflight, 0);
	atomic_set(&writer->rx_inflight, 0);
	return writer;
}

void cfs_extent_writer_release(struct cfs_extent_writer *writer)
{
	if (!writer)
		return;
	cancel_work_sync(&writer->tx_work);
	cancel_work_sync(&writer->rx_work);
	cfs_data_partition_release(writer->dp);
	if (writer->sock->enable_rdma) {
		cfs_rdma_release(writer->sock, false);
	} else {
		cfs_socket_release(writer->sock, false);
	}

	kfree(writer);
}

int cfs_extent_writer_flush(struct cfs_extent_writer *writer)
{
	struct cfs_extent_stream *es = writer->es;
	struct cfs_meta_client *meta = es->ec->meta;
	struct cfs_data_partition *dp = writer->dp;
	struct cfs_packet_extent_array discard_extents = { 0 };
	struct cfs_packet_extent ext;
	int ret;

	if (!cfs_extent_writer_test_dirty(writer))
		return 0;
	wait_event(writer->tx_wq, atomic_read(&writer->tx_inflight) == 0);
	wait_event(writer->rx_wq, atomic_read(&writer->rx_inflight) == 0);
	if (writer->ext_size == 0)
		return 0;
	cfs_packet_extent_init(&ext, writer->file_offset, dp->id,
			       writer->ext_id, 0, writer->ext_size);
	ret = cfs_extent_cache_append(&es->cache, &ext, true, &discard_extents, es->ec->log);
	if (unlikely(ret < 0)) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) append extent cache error %d\n",
			      es->ino, ret);
		return ret;
	}
	ret = cfs_meta_append_extent(meta, es->ino, &ext, &discard_extents);
	if (ret < 0) {
		cfs_log_error(es->ec->log,
			      "ino(%llu) sync extent cache error %d\n", es->ino,
			      ret);
		cfs_packet_extent_array_clear(&discard_extents);
		return ret;
	}
	cfs_extent_cache_remove_discard(&es->cache, &discard_extents);
	cfs_packet_extent_array_clear(&discard_extents);
	cfs_extent_writer_clear_dirty(writer);
	return 0;
}

void cfs_extent_writer_request(struct cfs_extent_writer *writer,
			       struct cfs_packet *packet)
{
	cfs_extent_writer_set_dirty(writer);
	cfs_extent_writer_write_bytes(writer,
				      be32_to_cpu(packet->request.hdr.size));
	spin_lock(&writer->lock_tx);
	list_add_tail(&packet->list, &writer->tx_packets);
	spin_unlock(&writer->lock_tx);
	atomic_inc(&writer->tx_inflight);
	queue_work(extent_work_queue, &writer->tx_work);
}

static void extent_writer_tx_work_cb(struct work_struct *work)
{
	struct cfs_extent_writer *writer =
		container_of(work, struct cfs_extent_writer, tx_work);
	struct cfs_packet *packet;
	int cnt = 0;

	while (true) {
		spin_lock(&writer->lock_tx);
		packet = list_first_entry_or_null(&writer->tx_packets,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		spin_unlock(&writer->lock_tx);
		if (!packet)
			break;

		if (!(writer->flags &
		      (EXTENT_WRITER_F_ERROR | EXTENT_WRITER_F_RECOVER))) {
			int ret;
			if (writer->sock->enable_rdma) {
				ret = cfs_rdma_send_packet(writer->sock,
							   packet);
			} else {
				ret = cfs_socket_send_packet(writer->sock,
							     packet);
			}

			if (ret < 0)
				writer->flags |= EXTENT_WRITER_F_RECOVER;
		}
		spin_lock(&writer->lock_rx);
		list_add_tail(&packet->list, &writer->rx_packets);
		spin_unlock(&writer->lock_rx);
		atomic_inc(&writer->rx_inflight);
		queue_work(extent_work_queue, &writer->rx_work);
	}
	atomic_sub(cnt, &writer->tx_inflight);
	wake_up(&writer->tx_wq);
}

static int extent_writer_recover(struct cfs_extent_writer *writer, struct cfs_packet *packet) {
	struct cfs_extent_writer *recover = writer->recover;
	struct cfs_extent_stream *es = writer->es;
	int ret = 0;

retry:
	if (!recover) {
		struct cfs_data_partition *dp;
		u64 ext_id;

		mutex_lock(&es->lock_writers);
		if (es->nr_writers >= es->max_writers) {
			mutex_unlock(&es->lock_writers);
			packet->error = -EPERM;
			cfs_log_error(es->ec->log, "nr_writers=%d >= max_writers=%d\n", es->nr_writers, es->max_writers);
			return -EPERM;
		}
		mutex_unlock(&es->lock_writers);

		ret = cfs_extent_id_new(es, &dp, &ext_id);
		if (ret < 0) {
			packet->error = ret;
			cfs_log_error(es->ec->log, "cfs_extent_id_new failed: %d\n", ret);
			return ret;
		}
		recover = cfs_extent_writer_new(
			es, dp,
			be64_to_cpu(packet->request.hdr.kernel_offset),
			ext_id, 0, 0);
		if (IS_ERR(recover)) {
			cfs_data_partition_release(dp);
			packet->error = -ENOMEM;
			cfs_log_error(es->ec->log, "cfs_extent_writer_new failed: %d\n", ret);
			return -ENOMEM;
		}

		mutex_lock(&es->lock_writers);
		list_add_tail(&recover->list, &es->writers);
		es->nr_writers++;
		mutex_unlock(&es->lock_writers);
		writer->recover = recover;
		cfs_log_debug(es->ec->log, "start recover writer. pid: %d ext_id: %d, recover file_offset: %d, reqid(%ld)\n",
			recover->dp->id, recover->ext_id, recover->file_offset, be64_to_cpu(packet->request.hdr.req_id));
	}

	packet->request.hdr.pid = cpu_to_be64(recover->dp->id);
	packet->request.hdr.ext_id = cpu_to_be64(recover->ext_id);
	packet->request.hdr.ext_offset = cpu_to_be64(
		be64_to_cpu(packet->request.hdr.kernel_offset) -
		recover->file_offset);
	packet->request.hdr.remaining_followers =
		recover->dp->nr_followers;
	if (es->enable_rdma) {
		ret = cfs_packet_set_request_arg(packet, recover->dp->rdma_follower_addrs);
	} else {
		ret = cfs_packet_set_request_arg(packet, recover->dp->follower_addrs);
	}
	if (unlikely(ret < 0)) {
		cfs_log_error(es->ec->log, "cfs_packet_set_request_arg failed: %d\n", ret);
		return ret;
	}

	packet->retry_count++;
	if (es->enable_rdma) {
		ret = do_extent_request_rdma(es, &recover->dp->members.base[0], packet);
	} else {
		ret = do_extent_request(es, &recover->dp->members.base[0], packet);
	}
	if (ret < 0 || packet->reply.hdr.result_code != CFS_STATUS_OK) {
		cfs_log_error(es->ec->log, "write recover failed. reqid: %ld, ext_id: %d, recover file_offset: %d, ext offset: %d, rc: 0x%x, retry: %d, ret: %d\n",
			be64_to_cpu(packet->request.hdr.req_id), recover->ext_id, recover->file_offset,
			be64_to_cpu(packet->request.hdr.ext_offset), packet->reply.hdr.result_code,
			packet->retry_count, ret);
		recover->flags |= EXTENT_WRITER_F_ERROR;
		writer->recover = NULL;
		recover = NULL;
		if (packet->retry_count <= REQUEST_RETRY_MAX) {
			goto retry;
		} else {
			cfs_log_error(es->ec->log, "packet reqid(%d) failed after %d retries\n",
				be64_to_cpu(packet->request.hdr.req_id), REQUEST_RETRY_MAX);
			return -EIO;
		}
	}
	// Update the writer pointer.
	packet->private = recover;

	return 0;
}

static void extent_writer_rx_work_cb(struct work_struct *work)
{
	struct cfs_extent_writer *writer =
		container_of(work, struct cfs_extent_writer, rx_work);
	struct cfs_extent_stream *es = writer->es;
	struct cfs_packet *packet;
	int cnt = 0;
	int ret;

	while (true) {
		spin_lock(&writer->lock_rx);
		packet = list_first_entry_or_null(&writer->rx_packets,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		spin_unlock(&writer->lock_rx);
		if (!packet)
			break;

		if (writer->flags & EXTENT_WRITER_F_ERROR) {
			packet->error = -EIO;
			goto handle_packet;
		}

		if (writer->flags & EXTENT_WRITER_F_RECOVER)
			goto recover_packet;

		if (writer->sock->enable_rdma) {
			ret = cfs_rdma_recv_packet(writer->sock, packet);
		} else {
			ret = cfs_socket_recv_packet(writer->sock, packet);
		}

		if (ret < 0 || packet->reply.hdr.result_code != CFS_STATUS_OK) {
			writer->flags |= EXTENT_WRITER_F_RECOVER;
			goto recover_packet;
		}
		goto handle_packet;

recover_packet:
		ret = extent_writer_recover(writer, packet);
		if (ret < 0) {
			cfs_log_error(es->ec->log, "extent_writer_recover failed: %d\n", ret);
			writer->flags |= EXTENT_WRITER_F_ERROR;
		}

handle_packet:
		if (packet->handle_reply)
			packet->handle_reply(packet);
		cfs_packet_release(packet);
	}
	atomic_sub(cnt, &writer->rx_inflight);
	wake_up(&writer->rx_wq);
}
