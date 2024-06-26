#ifndef RDMA_API_H_
#define RDMA_API_H_

#include <linux/module.h>
#include <linux/inet.h>
#include <linux/socket.h>
#include <linux/delay.h>
#include <linux/list.h>
#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <rdma/ib_cm.h>
#include "rdma_buffer.h"

#define IBVSOCKET_CONN_TIMEOUT_MS 5000
#define MSG_LEN 4096
#define BLOCK_NUM 32
#define TIMEOUT_JS 5000

enum IBVSocketConnState {
	IBVSOCKETCONNSTATE_UNCONNECTED = 0,
	IBVSOCKETCONNSTATE_CONNECTING = 1,
	IBVSOCKETCONNSTATE_ADDRESSRESOLVED = 2,
	IBVSOCKETCONNSTATE_ROUTERESOLVED = 3,
	IBVSOCKETCONNSTATE_ESTABLISHED = 4,
	IBVSOCKETCONNSTATE_FAILED = 5,
	IBVSOCKETCONNSTATE_REJECTED_STALE = 6,
	IBVSOCKETCONNSTATE_DESTROYED = 7
};
typedef enum IBVSocketConnState IBVSocketConnState_t;

struct IBVSocket {
	wait_queue_head_t eventWaitQ;
	struct rdma_cm_id *cm_id;
	struct ib_pd *pd;
	struct ib_cq *recvCQ; // recv completion queue
	struct ib_cq *sendCQ; // send completion queue
	struct ib_qp *qp; // send+recv queue pair
	struct BufferItem *recvBuf[BLOCK_NUM];
	int recvBufIndex;
	struct BufferItem *sendBuf[BLOCK_NUM];
	int sendBufIndex;
	struct mutex lock;
	volatile IBVSocketConnState_t connState;
};

extern struct IBVSocket *IBVSocket_construct(struct sockaddr_in *sin);
extern bool IBVSocket_destruct(struct IBVSocket *_this);
extern ssize_t IBVSocket_recvT(struct IBVSocket *_this, struct iov_iter *iter);
extern ssize_t IBVSocket_send(struct IBVSocket *_this, struct iov_iter *source);
extern struct BufferItem *IBVSocket_get_data_buf(struct IBVSocket *this, size_t size);
extern void IBVSocket_free_data_buf(struct IBVSocket *this, struct BufferItem *item);

#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#define MAX(a, b) (((a) < (b)) ? (b) : (a))

#endif