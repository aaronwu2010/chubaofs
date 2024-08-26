/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_extent.h"

extern struct workqueue_struct *extent_work_queue;

static void extent_reader_tx_work_cb(struct work_struct *work);
static void extent_reader_rx_work_cb(struct work_struct *work);

struct cfs_extent_reader *cfs_extent_reader_new(struct cfs_extent_stream *es,
						struct cfs_data_partition *dp,
						u32 host_idx, u64 ext_id)
{
	struct cfs_extent_reader *reader;
	int ret;

	BUG_ON(dp == NULL);
	reader = kzalloc(sizeof(*reader), GFP_NOFS);
	if (!reader)
		return ERR_PTR(-ENOMEM);
	host_idx = host_idx % dp->members.num;
	/*
	// For read action, the RDMA is not supported yet.
	if (es->enable_rdma) {
		ret = cfs_rdma_create(&dp->members.base[host_idx], es->ec->log,
				      &reader->sock, es->rdma_port);
	} else {
		ret = cfs_socket_create(&dp->members.base[host_idx],
					es->ec->log, &reader->sock);
	}
	*/
	ret = cfs_socket_create(&dp->members.base[host_idx],
				es->ec->log, &reader->sock);

	if (ret < 0) {
		kfree(reader);
		return ERR_PTR(ret);
	}
	reader->es = es;
	reader->dp = dp;
	reader->ext_id = ext_id;
	spin_lock_init(&reader->lock_tx);
	spin_lock_init(&reader->lock_rx);
	INIT_LIST_HEAD(&reader->tx_packets);
	INIT_LIST_HEAD(&reader->rx_packets);
	INIT_WORK(&reader->tx_work, extent_reader_tx_work_cb);
	INIT_WORK(&reader->rx_work, extent_reader_rx_work_cb);
	init_waitqueue_head(&reader->tx_wq);
	init_waitqueue_head(&reader->rx_wq);
	atomic_set(&reader->tx_inflight, 0);
	atomic_set(&reader->rx_inflight, 0);
	reader->host_idx = host_idx;
	return reader;
}

void cfs_extent_reader_release(struct cfs_extent_reader *reader)
{
	if (!reader)
		return;
	if (reader->recover) {
		cfs_extent_reader_release(reader->recover);
		reader->recover = NULL;
	}
	cancel_work_sync(&reader->tx_work);
	cancel_work_sync(&reader->rx_work);
	cfs_data_partition_release(reader->dp);
	if (reader->sock->enable_rdma) {
		cfs_rdma_release(reader->sock, false);
	} else {
		cfs_socket_release(reader->sock, false);
	}

	kfree(reader);
}

void cfs_extent_reader_flush(struct cfs_extent_reader *reader)
{
	wait_event(reader->tx_wq, atomic_read(&reader->tx_inflight) == 0);
	wait_event(reader->rx_wq, atomic_read(&reader->rx_inflight) == 0);
}

void cfs_extent_reader_request(struct cfs_extent_reader *reader,
			       struct cfs_packet *packet)
{
	spin_lock(&reader->lock_tx);
	list_add_tail(&packet->list, &reader->tx_packets);
	spin_unlock(&reader->lock_tx);
	atomic_inc(&reader->tx_inflight);
	queue_work(extent_work_queue, &reader->tx_work);
}

static void extent_reader_tx_work_cb(struct work_struct *work)
{
	struct cfs_extent_reader *reader =
		container_of(work, struct cfs_extent_reader, tx_work);
	struct cfs_packet *packet;
	int cnt = 0;

	while (true) {
		spin_lock(&reader->lock_tx);
		packet = list_first_entry_or_null(&reader->tx_packets,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		spin_unlock(&reader->lock_tx);
		if (!packet)
			break;

		if (!(reader->flags &
		      (EXTENT_READER_F_ERROR | EXTENT_READER_F_RECOVER))) {
			int ret;
			if (reader->sock->enable_rdma) {
				ret = cfs_rdma_send_packet(reader->sock,
							   packet);
			} else {
				ret = cfs_socket_send_packet(reader->sock,
							     packet);
			}

			if (ret == -ENOMEM) {
				reader->flags |= EXTENT_READER_F_ERROR;
				cfs_log_error(reader->es->ec->log, "send packet return -ENOMEM.\n");
			} else if (ret < 0) {
				reader->flags |= EXTENT_READER_F_RECOVER;
				cfs_log_error(reader->es->ec->log, "send packet error: %d.\n", ret);
			}
		}
		spin_lock(&reader->lock_rx);
		list_add_tail(&packet->list, &reader->rx_packets);
		spin_unlock(&reader->lock_rx);
		atomic_inc(&reader->rx_inflight);
		queue_work(extent_work_queue, &reader->rx_work);
	}
	atomic_sub(cnt, &reader->tx_inflight);
	wake_up(&reader->tx_wq);
}

static int extent_reader_recover(struct cfs_extent_reader *reader, struct cfs_packet *packet) {
	struct cfs_extent_stream *es = reader->es;
	struct cfs_extent_reader *recover = reader->recover;
	int ret = 0;

	if (!recover) {
		cfs_data_partition_get(reader->dp);
		recover = cfs_extent_reader_new(es, reader->dp,
						reader->host_idx + 1,
						reader->ext_id);
		if (IS_ERR(recover)) {
			cfs_data_partition_put(reader->dp);
			reader->flags |= EXTENT_READER_F_ERROR;
			packet->error = -ENOMEM;
			cfs_log_error(es->ec->log, "cfs_extent_reader_new failed: %d\n", ret);
			return -ENOMEM;
		}

		reader->recover = recover;
	}

	ret = do_extent_request_retry(es, recover->dp, packet, recover->dp->leader_idx);
	if (ret < 0) {
		cfs_log_error(es->ec->log, "do_extent_request_retry failed: %d\n", ret);
		cfs_extent_reader_release(recover);
		reader->recover = NULL;
		return ret;
	}
	cfs_data_partition_set_leader(recover->dp, ret);

	return 0;
}

static void extent_reader_rx_work_cb(struct work_struct *work)
{
	struct cfs_extent_reader *reader =
		container_of(work, struct cfs_extent_reader, rx_work);
	struct cfs_extent_stream *es = reader->es;
	struct cfs_packet *packet;
	int cnt = 0;
	int ret;

	while (true) {
		spin_lock(&reader->lock_rx);
		packet = list_first_entry_or_null(&reader->rx_packets,
						  struct cfs_packet, list);
		if (packet) {
			list_del(&packet->list);
			cnt++;
		}
		spin_unlock(&reader->lock_rx);
		if (!packet)
			break;

		if (reader->flags & EXTENT_READER_F_ERROR) {
			packet->error = -EIO;
			cfs_log_error(es->ec->log, "extent reader flags is EXTENT_READER_F_ERROR.\n");
			goto handle_packet;
		}

		if (reader->flags & EXTENT_READER_F_RECOVER)
			goto recover_packet;

		if (reader->sock->enable_rdma) {
			ret = cfs_rdma_recv_packet(reader->sock, packet);
		} else {
			ret = cfs_socket_recv_packet(reader->sock, packet);
		}

		if (ret < 0 || packet->reply.hdr.result_code != CFS_STATUS_OK) {
			reader->flags |= EXTENT_READER_F_RECOVER;
			goto recover_packet;
		}
		goto handle_packet;

recover_packet:
		ret = extent_reader_recover(reader, packet);
		if (ret < 0) {
			cfs_log_error(es->ec->log, "extent_reader_recover failed: %d\n", ret);
		}

handle_packet:
		if (packet->handle_reply)
			packet->handle_reply(packet);
		cfs_packet_release(packet);
	}
	atomic_sub(cnt, &reader->rx_inflight);
	wake_up(&reader->rx_wq);
}
