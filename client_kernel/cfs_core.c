#include "cfs_fs.h"
#include "cfs_packet.h"
#include "cfs_socket.h"

static int __init cfs_init(void)
{
	int ret;

	cfs_log_info("init\n");

	ret = cfs_socket_module_init();
	if (ret < 0) {
		cfs_log_err("init socket module error %d\n", ret);
		goto exit;
	}

	ret = cfs_packet_module_init();
	if (ret < 0) {
		cfs_log_err("init packet module error %d\n", ret);
		goto exit;
	}

	ret = cfs_extent_module_init();
	if (ret < 0) {
		cfs_log_err("init extent module error %d\n", ret);
		goto exit;
	}

	ret = cfs_fs_module_init();
	if (ret < 0) {
		cfs_log_err("init fs module error %d\n", ret);
		goto exit;
	}

	return register_filesystem(&cfs_fs_type);

exit:
	cfs_socket_module_exit();
	cfs_packet_module_exit();
	cfs_extent_module_exit();
	cfs_fs_module_exit();
	return ret;
}

static void __exit cfs_exit(void)
{
	cfs_log_info("exit\n");
	unregister_filesystem(&cfs_fs_type);
	cfs_packet_module_exit();
	cfs_socket_module_exit();
	cfs_extent_module_exit();
	cfs_fs_module_exit();
}

module_init(cfs_init);
module_exit(cfs_exit);

MODULE_VERSION("0.1");
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("CubeFS");