/*
 * Copyright 2023 The CubeFS Authors.
 */
#include <linux/proc_fs.h>

#include "cfs_fs.h"
#include "cfs_packet.h"
#include "cfs_socket.h"
#include "cfs_rdma_socket.h"

static int __init cfs_init(void)
{
	int ret;

	ret = cfs_socket_module_init();
	if (ret < 0) {
		cfs_pr_err("init socket module error %d\n", ret);
		goto exit;
	}

	ret = cfs_rdma_module_init();
	if (ret < 0) {
		cfs_pr_err("init rdma module error %d\n", ret);
		goto exit;
	}

	ret = cfs_packet_module_init();
	if (ret < 0) {
		cfs_pr_err("init packet module error %d\n", ret);
		goto exit;
	}

	ret = cfs_extent_module_init();
	if (ret < 0) {
		cfs_pr_err("init extent module error %d\n", ret);
		goto exit;
	}

	ret = cfs_fs_module_init();
	if (ret < 0) {
		cfs_pr_err("init fs module error %d\n", ret);
		goto exit;
	}

	ret = cfs_page_module_init();
	if (ret < 0) {
		cfs_pr_err("init page module error %d\n", ret);
		goto exit;
	}

	ret = register_filesystem(&cfs_fs_type);
	if (ret < 0) {
		cfs_pr_err("register file system error %d\n", ret);
		goto exit;
	}

	ret = cfs_global_log_init();
	if (ret) {
		cfs_pr_err("cfs_global_log_init failed: %d\n", ret);
		goto exit;
	}

	cfs_pr_info("init\n");
	return 0;

exit:
	cfs_socket_module_exit();
	cfs_rdma_module_exit();
	cfs_packet_module_exit();
	cfs_extent_module_exit();
	cfs_fs_module_exit();
	cfs_page_module_exit();
	cfs_global_log_exit();
	return ret;
}

static void __exit cfs_exit(void)
{
	int ret;

	ret = unregister_filesystem(&cfs_fs_type);
	if (ret < 0) {
		cfs_pr_err("unregister file system error %d\n", ret);
		return;
	}
	cfs_packet_module_exit();
	cfs_socket_module_exit();
	cfs_rdma_module_exit();
	cfs_extent_module_exit();
	cfs_fs_module_exit();
	cfs_page_module_exit();
	cfs_global_log_exit();
	cfs_pr_info("exit\n");
}

module_init(cfs_init);
module_exit(cfs_exit);

MODULE_VERSION("0.1");
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("CubeFS");
