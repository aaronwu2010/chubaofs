/*
 * Copyright 2023 The CubeFS Authors.
 */
#include "cfs_fs.h"

#define CFS_FS_MAGIC 0x20230705
#define CFS_BLOCK_SIZE_SHIFT 12
#define CFS_BLOCK_SIZE (1UL << CFS_BLOCK_SIZE_SHIFT)
#define CFS_INODE_MAX_ID ((1UL << 63) - 1)

#define CFS_UPDATE_LIMIT_INTERVAL_MS 5 * 60 * 1000u
#define CFS_LINKS_DEFAULT 20000000
#define CFS_LINKS_MIN 1000000

#define pr_qstr(q) \
	(q) ? (q)->len : 0, (q) ? (q)->name : (const unsigned char *)""

#define fmt_inode "%p{.ino=%lu,.imode=0%o,.uid=%u}"
#define pr_inode(inode)                                                       \
	(inode), (inode) ? (inode)->i_ino : 0, (inode) ? (inode)->i_mode : 0, \
		(inode) ? i_uid_read(inode) : 0

#define fmt_file "%p{}"
#define pr_file(file) (file)

#define fmt_dentry "%p{.name=%.*s}"
#define pr_dentry(d)                    \
	(d), (d) ? (d)->d_name.len : 0, \
		(d) ? (d)->d_name.name : (const unsigned char *)""

// #define ENABLE_XATTR

static struct kmem_cache *inode_cache;
static struct kmem_cache *pagevec_cache;

#define CFS_INODE(i) ((struct cfs_inode *)(i))

struct cfs_inode {
	struct inode vfs_inode;
	unsigned long revalidate_jiffies;
	unsigned long iattr_jiffies;
	unsigned long quota_jiffies;
	struct cfs_extent_stream *es;
	char *link_target;
	struct cfs_quota_info_array quota_infos;
};

struct cfs_file_info {
	char *marker;
	struct cfs_packet_dentry_array denties;
	size_t denties_offset;
	bool done;
};

static inline void cfs_file_info_release(struct cfs_file_info *fi)
{
	if (!fi)
		return;
	if (fi->marker)
		kfree(fi->marker);
	cfs_packet_dentry_array_clear(&fi->denties);
	kfree(fi);
}

static inline bool is_iattr_cache_valid(struct cfs_inode *ci)
{
	struct super_block *sb = ci->vfs_inode.i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	return ci->iattr_jiffies +
		       msecs_to_jiffies(cmi->options->attr_cache_valid_ms) >
	       jiffies;
}

static inline void update_iattr_cache(struct cfs_inode *ci)
{
	ci->iattr_jiffies = jiffies;
}

static inline void invalidate_iattr_cache(struct cfs_inode *ci)
{
	ci->iattr_jiffies = 0;
}

static inline bool is_dentry_cache_valid(struct cfs_inode *ci)
{
	struct super_block *sb = ci->vfs_inode.i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	return ci->revalidate_jiffies +
		       msecs_to_jiffies(cmi->options->dentry_cache_valid_ms) >
	       jiffies;
}

static inline void update_dentry_cache(struct cfs_inode *ci)
{
	ci->revalidate_jiffies = jiffies;
}

static inline void invalidate_dentry_cache(struct cfs_inode *ci)
{
	ci->revalidate_jiffies = 0;
}

static inline bool is_quota_cache_valid(struct cfs_inode *ci)
{
	struct super_block *sb = ci->vfs_inode.i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	return ci->quota_jiffies +
		       msecs_to_jiffies(cmi->options->quota_cache_valid_ms) >
	       jiffies;
}

static inline void update_quota_cache(struct cfs_inode *ci)
{
	ci->quota_jiffies = jiffies;
}

static inline void invalidate_quota_cache(struct cfs_inode *ci)
{
	ci->quota_jiffies = 0;
}

static inline bool is_links_exceed_limit(struct cfs_inode *ci)
{
	struct super_block *sb = ci->vfs_inode.i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	return ci->vfs_inode.i_nlink >= atomic_long_read(&cmi->links_limit);
}

static inline void cfs_inode_refresh_unlock(struct cfs_inode *ci,
					    struct cfs_packet_inode *iinfo)
{
	struct inode *inode = &ci->vfs_inode;

	inode->i_mode = iinfo->mode;
	inode->i_ctime = iinfo->create_time;
	inode->i_atime = iinfo->access_time;
	inode->i_mtime = iinfo->modify_time;
	i_uid_write(inode, iinfo->uid);
	i_gid_write(inode, iinfo->gid);
	set_nlink(inode, iinfo->nlink);
	inode->i_generation = iinfo->generation;
	i_size_write(inode, iinfo->size);
	cfs_quota_info_array_clear(&ci->quota_infos);
	cfs_quota_info_array_move(&ci->quota_infos, &iinfo->quota_infos);
	if (ci->link_target)
		kfree(ci->link_target);
	ci->link_target = cfs_move(iinfo->target, NULL);
}

static int cfs_inode_refresh(struct cfs_inode *ci)
{
	struct super_block *sb = ci->vfs_inode.i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct cfs_packet_inode *iinfo;
	int ret;

	ret = cfs_meta_get(cmi->meta, ci->vfs_inode.i_ino, &iinfo);
	if (ret < 0)
		return ret;
	spin_lock(&ci->vfs_inode.i_lock);
	cfs_inode_refresh_unlock(ci, iinfo);
	update_iattr_cache(ci);
	update_quota_cache(ci);
	update_dentry_cache(ci);
	spin_unlock(&ci->vfs_inode.i_lock);
	cfs_packet_inode_release(iinfo);
	return 0;
}

static struct inode *cfs_inode_new(struct super_block *sb,
				   struct cfs_packet_inode *iinfo, dev_t rdev)
{
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct cfs_inode *ci;
	struct inode *inode;

	ci = (struct cfs_inode *)iget_locked(sb, iinfo->ino);
	if (IS_ERR_OR_NULL(ci))
		return NULL;
	inode = &ci->vfs_inode;

	if (!(inode->i_state & I_NEW)) {
		cfs_pr_warning("old inode %p{.ino=%lu, .iprivate=%p}\n", inode,
			       inode->i_ino, inode->i_private);
		return inode;
	}

	cfs_inode_refresh_unlock(ci, iinfo);
	update_dentry_cache(ci);
	update_iattr_cache(ci);
	update_quota_cache(ci);

	/* timestamps updated by server */
	inode->i_flags |= S_NOATIME | S_NOCMTIME;
	inode->i_flags |= S_NOSEC;

	switch (inode->i_mode & S_IFMT) {
	case S_IFREG:
		inode->i_op = &cfs_file_iops;
		inode->i_fop = &cfs_file_fops;
		inode->i_data.a_ops = &cfs_address_ops;
		ci->es = cfs_extent_stream_new(cmi->ec, inode->i_ino);
		if (!ci->es) {
			iget_failed(inode);
			return NULL;
		}
		break;
	case S_IFDIR:
		inode->i_op = &cfs_dir_iops;
		inode->i_fop = &cfs_dir_fops;
		break;
	case S_IFLNK:
		inode->i_op = &cfs_symlink_iops;
		break;
	case S_IFIFO:
		inode->i_op = &cfs_special_iops;
		init_special_inode(inode, inode->i_mode, rdev);
		break;
	default:
		cfs_pr_err("unsupport inode mode 0%o\n", inode->i_mode);
		break;
	}
	unlock_new_inode(inode);
	return inode;
}

#ifdef KERNEL_READ_PAGE
static int cfs_readpage(struct file *file, struct page *page)
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;

	return cfs_extent_read_pages(ci->es, false, &page, 1, page_offset(page),
				     0, PAGE_SIZE);
}
#else
static int cfs_readfolio(struct file *file, struct folio *folio)
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct page *page = folio_page(folio, 0);

	return cfs_extent_read_pages(ci->es, false, &page, 1, page_offset(page),
				     0, PAGE_SIZE);
}
#endif

#ifdef KERNEL_HAS_FOLIO
bool cfs_dirty_folio(struct address_space *mapping, struct folio *folio) {
	struct page *page = folio_page(folio, 0);
	return __set_page_dirty_nobuffers(page);
}
#else
static int cfs_readpages_cb(void *data, struct page *page)
{
	struct cfs_page_vec *vec = data;
	struct inode *inode = page->mapping->host;
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	int ret;

	if (cfs_page_vec_append(vec, page))
		return 0;
	ret = cfs_extent_read_pages(ci->es, false, vec->pages, vec->nr,
				    page_offset(vec->pages[0]), 0, PAGE_SIZE);
	cfs_page_vec_clear(vec);
	if (ret < 0)
		goto failed;
	ret = cfs_page_vec_append(vec, page);
	BUG_ON(!ret);
	return 0;

failed:
	page_endio(page, READ, ret);
	return ret;
}

/**
 * Called by generic_file_aio_read(). Pages maybe discontinuous.
 */
static int cfs_readpages(struct file *file, struct address_space *mapping,
			 struct list_head *pages, unsigned nr_pages)
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct cfs_page_vec *vec;
	int ret;

	vec = cfs_page_vec_new();
	if (!vec)
		return -ENOMEM;
	ret = read_cache_pages(mapping, pages, cfs_readpages_cb, vec);
	if (ret < 0)
		goto out;
	if (cfs_page_vec_empty(vec)) {
		ret = 0;
		goto out;
	}
	ret = cfs_extent_read_pages(ci->es, false, vec->pages, vec->nr,
				    page_offset(vec->pages[0]), 0, PAGE_SIZE);

out:
	cfs_page_vec_release(vec);
	return ret;
}
#endif

static inline loff_t cfs_inode_page_size(struct cfs_inode *ci,
					 struct page *page)
{
	loff_t offset = page_offset(page);

	return min((loff_t)PAGE_SIZE, i_size_read(&ci->vfs_inode) - offset);
}

static int cfs_writepage(struct page *page, struct writeback_control *wbc)
{
	struct inode *inode = page->mapping->host;
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	loff_t page_size;

	page_size = cfs_inode_page_size(ci, page);
	set_page_writeback(page);
	return cfs_extent_write_pages(ci->es, &page, 1, page_offset(page), 0,
				      page_size);
}

static int cfs_writepages_cb(struct page *page, struct writeback_control *wbc,
			     void *data)
{
	struct inode *inode = page->mapping->host;
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct cfs_page_vec *vec = data;
	loff_t page_size;
	int ret;

	if (!cfs_page_vec_append(vec, page)) {
		page_size = cfs_inode_page_size(ci, vec->pages[vec->nr - 1]);
		ret = cfs_extent_write_pages(ci->es, vec->pages, vec->nr,
					     page_offset(vec->pages[0]), 0,
					     page_size);
		cfs_page_vec_clear(vec);
		if (ret < 0) {
			unlock_page(page);
			return ret;
		}
		ret = cfs_page_vec_append(vec, page);
		BUG_ON(!ret);
	}
	set_page_writeback(page);
	return 0;
}

/**
 * Called by flush()/fsync(). Pages maybe discontinuous.
 * Caller not holds the i_mutex.
 */
static int cfs_writepages(struct address_space *mapping,
			  struct writeback_control *wbc)
{
	struct inode *inode = mapping->host;
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct cfs_page_vec *vec;
	loff_t page_size;
	int ret = 0;

	vec = cfs_page_vec_new();
	if (!vec)
		return -ENOMEM;
	write_cache_pages(mapping, wbc, cfs_writepages_cb, vec);
	if (!cfs_page_vec_empty(vec)) {
		page_size = cfs_inode_page_size(ci, vec->pages[vec->nr - 1]);
		ret = cfs_extent_write_pages(ci->es, vec->pages, vec->nr,
					     page_offset(vec->pages[0]), 0,
					     page_size);
	}
	cfs_page_vec_release(vec);
	return ret;
}

/**
 * Called by generic_file_aio_write(), caller holds the i_mutex.
 */
#ifdef KERNEL_WRITE_GEGIN_NO_FLAGS
static int cfs_write_begin(struct file *file, struct address_space *mapping,
			   loff_t pos, unsigned len,
			   struct page **pagep, void **fsdata)
#else
static int cfs_write_begin(struct file *file, struct address_space *mapping,
			   loff_t pos, unsigned len, unsigned flags,
			   struct page **pagep, void **fsdata)
#endif
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	pgoff_t index = pos >> PAGE_SHIFT;
	loff_t page_off = pos & PAGE_MASK;
	int pos_in_page = pos & ~PAGE_MASK;
	int end_in_page = pos_in_page + len;
	struct page *page;
	loff_t i_size;
	int ret;

	/**
	 * find or create a locked page.
	 */
#ifdef KERNEL_WRITE_GEGIN_NO_FLAGS
	page = grab_cache_page_write_begin(mapping, index);
	if (!page)
		return -ENOMEM;
#else
	page = grab_cache_page_write_begin(mapping, index, flags);
	if (!page)
		return -ENOMEM;
#endif

	wait_on_page_writeback(page);

	*pagep = page;

	/**
	 * 1. uptodate page write.
	 */
	if (PageUptodate(page))
		return 0;

	/**
	 * 2. full page write.
	 */
	if (pos_in_page == 0 && len == PAGE_SIZE)
		return 0;

	/**
	 * 3. end of file.
	 */
	i_size = i_size_read(inode);
	if (page_off >= i_size || (pos_in_page == 0 && (pos + len) >= i_size &&
				   end_in_page - pos_in_page != PAGE_SIZE)) {
		zero_user_segments(page, 0, pos_in_page, end_in_page,
				   PAGE_SIZE);
		return 0;
	}

	/**
	 * 4. uncached page write, page must be read from server first.
	 */
	ret = cfs_extent_read_pages(ci->es, false, &page, 1, page_offset(page),
				    0, PAGE_SIZE);
	lock_page(page);
	if (PageError(page))
		ret = -EIO;
	if (ret < 0) {
		unlock_page(page);
		put_page(page);
	}
	return ret;
}

/**
 * Called by generic_file_aio_write(), Caller holds the i_mutex.
 */
static int cfs_write_end(struct file *file, struct address_space *mapping,
			 loff_t pos, unsigned len, unsigned copied,
			 struct page *page, void *fsdata)
{
	struct inode *inode = page->mapping->host;
	loff_t last_pos = pos + copied;

	if (copied < len) {
		unsigned from = pos & (PAGE_SIZE - 1);

		zero_user(page, from + copied, len - copied);
	}

	if (!PageUptodate(page))
		SetPageUptodate(page);

	if (last_pos > i_size_read(inode))
		i_size_write(inode, last_pos);

	set_page_dirty(page);
	unlock_page(page);
	put_page(page);

	return copied;
}

/**
  * Called by generic_file_aio_write(), Caller holds the i_mutex.
  */
#if defined(KERNEL_HAS_DIO_WITH_ITER)
static ssize_t cfs_direct_io(struct kiocb *iocb, struct iov_iter *iter)
{
	struct file *file = iocb->ki_filp;
	struct inode *inode = file_inode(file);
	loff_t offset = iocb->ki_pos;

	return cfs_extent_direct_io(CFS_INODE(inode)->es, iov_iter_rw(iter), iter, offset);
}
#elif defined(KERNEL_HAS_DIO_WITH_ITER_AND_OFFSET)
static ssize_t cfs_direct_io(struct kiocb *iocb, struct iov_iter *iter,
			     loff_t offset)
{
	struct file *file = iocb->ki_filp;
	struct inode *inode = file_inode(file);

	return cfs_extent_direct_io(CFS_INODE(inode)->es, iov_iter_rw(iter), iter, offset);
}
#else
static ssize_t cfs_direct_io(int type, struct kiocb *iocb,
			     const struct iovec *iov, loff_t offset,
			     unsigned long nr_segs)
{
	struct file *file = iocb->ki_filp;
	struct inode *inode = file_inode(file);
	struct iov_iter iter;

#ifdef KERNEL_HAS_IOV_ITER_WITH_TAG
	iov_iter_init(&iter, type, iov, nr_segs, iov_length(iov, nr_segs));
#else
	iov_iter_init(&iter, iov, nr_segs, iov_length(iov, nr_segs), 0);
#endif
	return cfs_extent_direct_io(CFS_INODE(inode)->es, type, &iter, offset);
}
#endif

static int cfs_open(struct inode *inode, struct file *file)
{
	struct cfs_file_info *cfi = file->private_data;
	struct super_block *sb = inode->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	int ret = 0;

	if (cfi) {
		cfs_log_warn(cmi->log, "open file %p is already opened\n",
			     file);
		return 0;
	}
	cfi = kzalloc(sizeof(*cfi), GFP_NOFS);
	if (!cfi) {
		ret = -ENOMEM;
		goto out;
	}
	switch (inode->i_mode & S_IFMT) {
	case S_IFDIR:
		cfi->marker = kstrdup("", GFP_KERNEL);
		if (!cfi->marker) {
			kfree(cfi);
			ret = -ENOMEM;
			goto out;
		}
		break;

	default:
		break;
	}
	file->private_data = cfi;
#if defined(KERNEL_HAS_ITERATE_DIR) && defined(FMODE_KABI_ITERATE)
	file->f_mode |= FMODE_KABI_ITERATE;
#endif

out:
	cfs_log_debug(cmi->log,
		      "file=" fmt_file ", inode=" fmt_inode
		      ", dentry=" fmt_dentry ", err=%d\n",
		      pr_file(file), pr_inode(inode),
		      pr_dentry(file_dentry(file)), ret);
	return ret;
}

static int cfs_release(struct inode *inode, struct file *file)
{
	struct cfs_file_info *cfi = file->private_data;
	struct super_block *sb = inode->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	cfs_log_debug(cmi->log,
		      "file=" fmt_file ", inode=" fmt_inode
		      ", dentry=" fmt_dentry "\n",
		      pr_file(file), pr_inode(inode),
		      pr_dentry(file_dentry(file)));
	if (!cfi)
		return 0;
	if (cfi->marker)
		kfree(cfi->marker);
	cfs_packet_dentry_array_clear(&cfi->denties);
	kfree(cfi);
	file->private_data = NULL;
	return 0;
}

static int cfs_flush(struct file *file, fl_owner_t id)
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct super_block *sb = inode->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	ktime_t time;
	int ret;

	time = ktime_get();
	ret = write_inode_now(inode, 1);
	if (ret < 0) {
		cfs_log_error(cmi->log, "write inode(%llu) error: %d\n",
			      inode->i_ino, ret);
		goto out;
	}
	ret = cfs_extent_stream_flush(ci->es);
	if (ret < 0) {
		cfs_log_error(cmi->log, "flush inode(%llu) error: %d\n",
			      inode->i_ino, ret);
		goto out;
	}

out:
	cfs_log_debug(cmi->log, "file=" fmt_file ", elapsed=%llu us, err=%d\n",
		      pr_file(file), ktime_us_delta(ktime_get(), time), ret);
	return ret;
}

static int cfs_fsync(struct file *file, loff_t start, loff_t end, int datasync)
{
	struct inode *inode = file_inode(file);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct super_block *sb = inode->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	ktime_t time;
	int ret;

	time = ktime_get();
	ret = filemap_write_and_wait_range(file->f_mapping, start, end);
	if (ret < 0) {
		cfs_log_error(cmi->log, "write inode(%llu) error\n",
			      inode->i_ino, ret);
		goto out;
	}
	ret = cfs_extent_stream_flush(ci->es);
	if (ret < 0) {
		cfs_log_error(cmi->log, "flush inode(%llu) error\n",
			      inode->i_ino, ret);
		goto out;
	}

out:
	cfs_log_debug(cmi->log, "file=" fmt_file ", elapsed=%llu us, err=%d\n",
		      pr_file(file), ktime_us_delta(ktime_get(), time), ret);
	return ret;
}

#define READDIR_NUM 1024
#if defined(KERNEL_HAS_ITERATE_DIR_SHARED) || defined(KERNEL_HAS_ITERATE_DIR)
static int cfs_iterate_dir(struct file *file, struct dir_context *ctx)
#else
static int cfs_readdir(struct file *file, void *dirent, filldir_t filldir)
#endif
{
	struct inode *inode = file_inode(file);
	struct super_block *sb = inode->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct cfs_file_info *cfi = file->private_data;
	struct cfs_packet_dentry *dentry;
	ktime_t time;
	size_t i;
	int ret = 0;

	time = ktime_get();
#if defined(KERNEL_HAS_ITERATE_DIR_SHARED) || defined(KERNEL_HAS_ITERATE_DIR)
	if (!dir_emit_dots(file, ctx)) {
		ret = 0;
		goto out;
	}
	for (; cfi->denties_offset < cfi->denties.num; cfi->denties_offset++) {
		dentry = &cfi->denties.base[cfi->denties_offset];
		if (!dir_emit(ctx, dentry->name, strlen(dentry->name),
			      dentry->ino, (dentry->type >> 12) & 15)) {
			ret = 0;
			goto out;
		}
		ctx->pos++;
	}
#else
	if (file->f_pos == 0) {
		if (filldir(dirent, ".", 1, 0, inode->i_ino, DT_DIR) < 0) {
			ret = 0;
			goto out;
		}
		file->f_pos = 1;
	}
	if (file->f_pos == 1) {
		if (filldir(dirent, "..", 2, 1, parent_ino(file->f_path.dentry),
			    DT_DIR) < 0) {
			ret = 0;
			goto out;
		}
		file->f_pos = 2;
	}
	for (; cfi->denties_offset < cfi->denties.num; cfi->denties_offset++) {
		dentry = &cfi->denties.base[cfi->denties_offset];
		if (filldir(dirent, dentry->name, strlen(dentry->name),
			    file->f_pos, dentry->ino,
			    (dentry->type >> 12) & 15) < 0) {
			ret = 0;
			goto out;
		}
		file->f_pos++;
	}
#endif

	while (!cfi->done) {
		struct u64_array ino_vec;
		struct cfs_packet_inode_ptr_array iinfo_vec;

		if (cfi->denties.num > 0) {
			kfree(cfi->marker);
			cfi->marker = kstrdup(
				cfi->denties.base[cfi->denties.num - 1].name,
				GFP_KERNEL);
			if (!cfi->marker) {
				ret = -ENOMEM;
				goto out;
			}

			cfs_packet_dentry_array_clear(&cfi->denties);
		}
		ret = cfs_meta_readdir(cmi->meta, inode->i_ino, cfi->marker,
				       READDIR_NUM, &cfi->denties);
		if (ret < 0) {
			cfs_log_error(cmi->log, "readdir error %d\n", ret);
			goto out;
		}
		if (cfi->denties.num < READDIR_NUM)
			cfi->done = true;

		ret = u64_array_init(&ino_vec, cfi->denties.num);
		if (ret < 0)
			goto out;
		for (i = 0; i < ino_vec.cap; i++) {
			ino_vec.base[ino_vec.num++] = cfi->denties.base[i].ino;
		}
		ret = cfs_meta_batch_get(cmi->meta, &ino_vec, &iinfo_vec);
		u64_array_clear(&ino_vec);
		if (ret < 0)
			goto out;

		for (i = 0; i < iinfo_vec.num; i++) {
			struct cfs_inode *ci;

			ci = (struct cfs_inode *)ilookup(
				sb, iinfo_vec.base[i]->ino);
			if (!ci)
				continue;
			spin_lock(&ci->vfs_inode.i_lock);
			cfs_inode_refresh_unlock(ci, iinfo_vec.base[i]);
			update_iattr_cache(ci);
			update_quota_cache(ci);
			update_dentry_cache(ci);
			spin_unlock(&ci->vfs_inode.i_lock);
			iput(&ci->vfs_inode);
		}
		/**
		 * free cfs_packet_inode array.
		 */
		cfs_packet_inode_ptr_array_clear(&iinfo_vec);

		for (cfi->denties_offset = 0;
		     cfi->denties_offset < cfi->denties.num;
		     cfi->denties_offset++) {
			dentry = &cfi->denties.base[cfi->denties_offset];
#if defined(KERNEL_HAS_ITERATE_DIR_SHARED) || defined(KERNEL_HAS_ITERATE_DIR)
			if (!dir_emit(ctx, dentry->name, strlen(dentry->name),
				      dentry->ino, (dentry->type >> 12) & 15)) {
				ret = 0;
				goto out;
			}
			ctx->pos++;
#else
			if (filldir(dirent, dentry->name, strlen(dentry->name),
				    file->f_pos, dentry->ino,
				    (dentry->type >> 12) & 15) < 0) {
				ret = 0;
				goto out;
			}
			file->f_pos++;
#endif
		}
	}

out:
	cfs_log_debug(
		cmi->log,
		"file=" fmt_file ", inode=" fmt_inode ", dentry=" fmt_dentry
		", offset=%zu, nr_dentry=%zu, done=%d, elapsed=%llu us, err=%d\n",
		pr_file(file), pr_inode(inode), pr_dentry(file_dentry(file)),
		cfi->denties_offset, cfi->denties.num, cfi->done,
		ktime_us_delta(ktime_get(), time), ret);
	return 0;
}

/**
 * Directory Entry Cache
 */
static int cfs_d_revalidate(struct dentry *dentry, unsigned int flags)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct inode *inode = d_inode(dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	int ret = 0;

	if (flags & LOOKUP_RCU)
		return -ECHILD;

	if (!inode)
		return true;

	if (!is_dentry_cache_valid(ci)) {
		ret = cfs_meta_get(cmi->meta, inode->i_ino, NULL);
		if (ret == -ENOENT) {
			update_dentry_cache(ci);
			return false;
		} else if (ret < 0) {
			cfs_log_warn(cmi->log,
				     "get inode(%lu) error %d, try again\n",
				     inode->i_ino, ret);
			return true;
		} else {
			update_dentry_cache(ci);
			return true;
		}
	}
	return true;
}

/**
 * File Inode
 */
#ifdef KERNEL_HAS_NAMESPACE
static int cfs_permission(struct user_namespace *ns, struct inode *inode, int mask)
{
	if (mask & MAY_NOT_BLOCK)
		return -ECHILD;
	return generic_permission(ns, inode, mask);
}
#else
static int cfs_permission(struct inode *inode, int mask)
{
	if (mask & MAY_NOT_BLOCK)
		return -ECHILD;
	return generic_permission(inode, mask);
}
#endif

#ifdef KERNEL_HAS_NAMESPACE
static int cfs_setattr(struct user_namespace *ns, struct dentry *dentry, struct iattr *iattr)
#else
static int cfs_setattr(struct dentry *dentry, struct iattr *iattr)
#endif
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct inode *inode = d_inode(dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	ktime_t time;
	int err;

	time = ktime_get();
#ifdef KERNEL_HAS_NAMESPACE
	err = setattr_prepare(ns, dentry, iattr);
#elif defined(KERNEL_HAS_SETATTR_PREPARE)
	err = setattr_prepare(dentry, iattr);
#else
	err = inode_change_ok(inode, iattr);
#endif
	if (err)
		goto out;

	if (iattr->ia_valid & ATTR_SIZE) {
		truncate_setsize(inode, iattr->ia_size);
		err = cfs_extent_stream_truncate(ci->es, iattr->ia_size);
		if (err)
			goto out;
	}

	if (ia_valid_to_u32(iattr->ia_valid) != 0) {
		err = cfs_meta_set_attr(cmi->meta, inode->i_ino, iattr);
		if (err)
			goto out;
	}

#ifdef KERNEL_HAS_NAMESPACE
	setattr_copy(ns, inode, iattr);
#else
	setattr_copy(inode, iattr);
#endif
	mark_inode_dirty(inode);

out:
	cfs_log_debug(cmi->log,
		      "dentry=" fmt_dentry ", inode=" fmt_inode
		      ", ia_valid=0x%x, elapsed=%llu us, err=%d\n",
		      pr_dentry(dentry), pr_inode(inode), iattr->ia_valid,
		      ktime_us_delta(ktime_get(), time), err);
	return err;
}

#ifdef KERNEL_HAS_NAMESPACE
static int cfs_getattr(struct user_namespace *ns, const struct path *path, struct kstat *stat,
		       u32 request_mask, unsigned int query_flags)
{
	struct inode *inode = d_inode(path->dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;

	if (!is_iattr_cache_valid(ci))
		cfs_inode_refresh(ci);
	generic_fillattr(ns, inode, stat);
	return 0;
}
#elif defined(KERNEL_HAS_GETATTR_WITH_PATH)
static int cfs_getattr(const struct path *path, struct kstat *stat,
		       u32 request_mask, unsigned int query_flags)
{
	struct inode *inode = d_inode(path->dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;

	if (!is_iattr_cache_valid(ci))
		cfs_inode_refresh(ci);
	generic_fillattr(inode, stat);
	return 0;
}
#else
static int cfs_getattr(struct vfsmount *mnt, struct dentry *dentry,
		       struct kstat *stat)
{
	struct inode *inode = d_inode(dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;

	if (!is_iattr_cache_valid(ci))
		cfs_inode_refresh(ci);
	generic_fillattr(inode, stat);
	return 0;
}
#endif

#ifdef ENABLE_XATTR
static int cfs_setxattr(struct dentry *dentry, const char *name,
			const void *value, size_t len, int flags)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	u64 ino = dentry->d_inode->i_ino;

	cfs_log_debug(cmi->log,
		      "dentry=" fmt_dentry
		      ", name=%s, value=%.*s, flags=0x%x\n",
		      pr_dentry(dentry), name, (int)len, (const char *)value,
		      flags);
	return cfs_meta_set_xattr(cmi->meta, ino, name, value, len, flags);
}

static ssize_t cfs_getxattr(struct dentry *dentry, const char *name,
			    void *value, size_t size)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	u64 ino = dentry->d_inode->i_ino;

	cfs_log_debug(cmi->log, "dentry=" fmt_dentry ", name=%s\n",
		      pr_dentry(dentry), name);
	return cfs_meta_get_xattr(cmi->meta, ino, name, value, size);
}

static ssize_t cfs_listxattr(struct dentry *dentry, char *names, size_t size)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	u64 ino = dentry->d_inode->i_ino;

	cfs_log_debug(cmi->log, "dentry=" fmt_dentry "\n", pr_dentry(dentry));
	return cfs_meta_list_xattr(cmi->meta, ino, names, size);
}

static int cfs_removexattr(struct dentry *dentry, const char *name)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	u64 ino = dentry->d_inode->i_ino;

	cfs_log_debug(cmi->log, "dentry=" fmt_dentry ", name=%s\n",
		      pr_dentry(dentry), name);
	return cfs_meta_remove_xattr(cmi->meta, ino, name);
}
#endif

static struct dentry *cfs_lookup(struct inode *dir, struct dentry *dentry,
				 unsigned int flags)
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct cfs_packet_inode *iinfo;
	struct inode *inode;
	struct dentry *new_dentry;
	ktime_t time;
	int ret;

	time = ktime_get();
	if (unlikely(dentry->d_name.len > NAME_MAX)) {
		ret = -ENAMETOOLONG;
		new_dentry = ERR_PTR(ret);
		goto out;
	}

	ret = cfs_meta_lookup(cmi->meta, dir->i_ino, &dentry->d_name, &iinfo);
	if (ret == -ENOENT) {
		d_add(dentry, NULL);
		new_dentry = NULL;
		goto out;
	} else if (ret < 0) {
		cfs_log_error(cmi->log, "lookup inode '%.*s', error %d\n",
			      pr_qstr(&dentry->d_name), ret);
		new_dentry = ERR_PTR(ret);
		goto out;
	}

	inode = cfs_inode_new(sb, iinfo, 0);
	cfs_packet_inode_release(iinfo);
	if (!inode) {
		cfs_log_error(cmi->log, "create inode '%.*s' failed\n",
			      pr_qstr(&dentry->d_name));
		d_add(dentry, NULL);
		new_dentry = NULL;
		goto out;
	}
	new_dentry = d_splice_alias(inode, dentry);

out:
	cfs_log_debug(cmi->log,
		      "dir=" fmt_inode ", dentry=" fmt_dentry
		      ", flags=0x%x, elapsed=%llu us, err=%d\n",
		      pr_inode(dir), pr_dentry(dentry), flags,
		      ktime_us_delta(ktime_get(), time), ret);
	return new_dentry;
}

#ifdef KERNEL_HAS_NAMESPACE
static int cfs_create(struct user_namespace *ns, struct inode *dir, struct dentry *dentry, umode_t mode,
		      bool excl)
#else
static int cfs_create(struct inode *dir, struct dentry *dentry, umode_t mode,
		      bool excl)
#endif
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	uid_t uid = from_kuid(&init_user_ns, current_fsuid());
	uid_t gid = from_kgid(&init_user_ns, current_fsgid());
	struct cfs_quota_info_array *quota = NULL;
	struct cfs_packet_inode *iinfo;
	struct inode *inode = NULL;
	ktime_t time;
	int ret;

	time = ktime_get();
	ret = cfs_inode_refresh(CFS_INODE(dir));
	if (ret < 0)
		goto out;

	if (is_links_exceed_limit(CFS_INODE(dir))) {
		ret = -EDQUOT;
		goto out;
	}

	if (cmi->options->enable_quota) {
		if (!is_quota_cache_valid(CFS_INODE(dir)))
			cfs_inode_refresh(CFS_INODE(dir));
		quota = &CFS_INODE(dir)->quota_infos;
	}

	ret = cfs_meta_create(cmi->meta, dir->i_ino, &dentry->d_name, mode, uid,
			      gid, NULL, quota, &iinfo);
	if (ret < 0) {
		cfs_log_error(cmi->log, "create dentry error %d\n", ret);
		goto out;
	}

	inode = cfs_inode_new(sb, iinfo, 0);
	cfs_packet_inode_release(iinfo);
	if (!inode) {
		ret = -ENOMEM;
		goto out;
	}
	d_instantiate(dentry, inode);
	invalidate_iattr_cache(CFS_INODE(dir));

out:
	cfs_log_audit(cmi->log, "Create", dentry, NULL, ret,
		      ktime_us_delta(ktime_get(), time),
		      inode ? inode->i_ino : 0, 0);
	return ret;
}

static int cfs_link(struct dentry *src_dentry, struct inode *dst_dir,
		    struct dentry *dst_dentry)
{
	struct super_block *sb = dst_dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	ktime_t time;
	int ret;

	time = ktime_get();
	ret = cfs_inode_refresh(CFS_INODE(dst_dir));
	if (ret < 0)
		goto out;

	if (is_links_exceed_limit(CFS_INODE(dst_dir))) {
		ret = -EDQUOT;
		goto out;
	}

	ret = cfs_meta_link(cmi->meta, dst_dir->i_ino, &dst_dentry->d_name,
			    src_dentry->d_inode->i_ino, NULL);
	if (ret < 0)
		goto out;

	ihold(src_dentry->d_inode);
	d_instantiate(dst_dentry, src_dentry->d_inode);
	invalidate_iattr_cache(CFS_INODE(dst_dir));
	invalidate_iattr_cache(CFS_INODE(src_dentry->d_inode));

out:
	cfs_log_audit(cmi->log, "Link", src_dentry, dst_dentry, ret,
		      ktime_us_delta(ktime_get(), time),
		      src_dentry->d_inode->i_ino, src_dentry->d_inode->i_ino);
	if (ret)
		d_drop(dst_dentry);
	return ret;
}

#ifdef KERNEL_HAS_NAMESPACE
static int cfs_symlink(struct user_namespace *ns, struct inode *dir, struct dentry *dentry,
		       const char *target)
#else
static int cfs_symlink(struct inode *dir, struct dentry *dentry,
		       const char *target)
#endif
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	uid_t uid = from_kuid(&init_user_ns, current_fsuid());
	uid_t gid = from_kgid(&init_user_ns, current_fsgid());
	struct cfs_quota_info_array *quota = NULL;
	umode_t mode;
	struct cfs_packet_inode *iinfo;
	struct inode *inode = NULL;
	ktime_t time;
	int ret;

	time = ktime_get();
	ret = cfs_inode_refresh(CFS_INODE(dir));
	if (ret < 0)
		goto out;

	if (is_links_exceed_limit(CFS_INODE(dir))) {
		ret = -EDQUOT;
		goto out;
	}

	if (cmi->options->enable_quota) {
		if (!is_quota_cache_valid(CFS_INODE(dir)))
			cfs_inode_refresh(CFS_INODE(dir));
		quota = &CFS_INODE(dir)->quota_infos;
	}

	mode = S_IFLNK | S_IRWXU | S_IRWXG | S_IRWXO;
	ret = cfs_meta_create(cmi->meta, dir->i_ino, &dentry->d_name, mode, uid,
			      gid, target, quota, &iinfo);
	if (ret < 0) {
		cfs_log_error(cmi->log, "create dentry error %d\n", ret);
		goto out;
	}

	inode = cfs_inode_new(sb, iinfo, 0);
	cfs_packet_inode_release(iinfo);
	if (!inode) {
		ret = -ENOMEM;
		goto out;
	}

	d_instantiate(dentry, inode);
	invalidate_iattr_cache(CFS_INODE(dir));

out:
	cfs_log_audit(cmi->log, "Symlink", dentry, NULL, ret,
		      ktime_us_delta(ktime_get(), time),
		      inode ? inode->i_ino : 0, 0);
	return ret;
}

#ifdef KERNEL_HAS_NAMESPACE
static int cfs_mkdir(struct user_namespace *ns, struct inode *dir, struct dentry *dentry, umode_t mode)
#else
static int cfs_mkdir(struct inode *dir, struct dentry *dentry, umode_t mode)
#endif
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	uid_t uid = from_kuid(&init_user_ns, current_fsuid());
	uid_t gid = from_kgid(&init_user_ns, current_fsgid());
	struct cfs_quota_info_array *quota = NULL;
	struct cfs_packet_inode *iinfo;
	struct inode *inode = NULL;
	ktime_t time;
	int ret;

	time = ktime_get();
	ret = cfs_inode_refresh(CFS_INODE(dir));
	if (ret < 0)
		goto out;

	if (is_links_exceed_limit(CFS_INODE(dir))) {
		ret = -EDQUOT;
		goto out;
	}

	if (cmi->options->enable_quota) {
		if (!is_quota_cache_valid(CFS_INODE(dir)))
			cfs_inode_refresh(CFS_INODE(dir));
		quota = &CFS_INODE(dir)->quota_infos;
	}

	mode &= ~current_umask();
	mode |= S_IFDIR;
	ret = cfs_meta_create(cmi->meta, dir->i_ino, &dentry->d_name, mode, uid,
			      gid, NULL, quota, &iinfo);
	if (ret < 0) {
		cfs_log_error(cmi->log, "create dentry error %d\n", ret);
		goto out;
	}

	inode = cfs_inode_new(sb, iinfo, 0);
	cfs_packet_inode_release(iinfo);
	if (!inode) {
		ret = -ENOMEM;
		goto out;
	}
	d_instantiate(dentry, inode);
	invalidate_iattr_cache(CFS_INODE(dir));
out:
	cfs_log_audit(cmi->log, "Mkdir", dentry, NULL, ret,
		      ktime_us_delta(ktime_get(), time),
		      inode ? inode->i_ino : 0, 0);
	return ret;
}

static int cfs_rmdir(struct inode *dir, struct dentry *dentry)
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	ktime_t time;
	u64 ino = 0;
	int ret;

	time = ktime_get();
	ret = cfs_meta_delete(cmi->meta, dir->i_ino, &dentry->d_name,
			      d_is_dir(dentry), &ino);
	invalidate_iattr_cache(CFS_INODE(dir));
	cfs_log_audit(cmi->log, "Rmdir", dentry, NULL, ret,
		      ktime_us_delta(ktime_get(), time), ino, 0);
	return ret;
}

#ifdef KERNEL_HAS_NAMESPACE
static int cfs_mknod(struct user_namespace *ns, struct inode *dir, struct dentry *dentry, umode_t mode,
		     dev_t rdev)
#else
static int cfs_mknod(struct inode *dir, struct dentry *dentry, umode_t mode,
		     dev_t rdev)
#endif
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	uid_t uid = from_kuid(&init_user_ns, current_fsuid());
	uid_t gid = from_kgid(&init_user_ns, current_fsgid());
	struct cfs_quota_info_array *quota = NULL;
	struct cfs_packet_inode *iinfo;
	struct inode *inode = NULL;
	ktime_t time;
	int ret;

	time = ktime_get();
	ret = cfs_inode_refresh(CFS_INODE(dir));
	if (ret < 0)
		goto out;

	if (is_links_exceed_limit(CFS_INODE(dir))) {
		ret = -EDQUOT;
		goto out;
	}

	if (cmi->options->enable_quota) {
		if (!is_quota_cache_valid(CFS_INODE(dir)))
			cfs_inode_refresh(CFS_INODE(dir));
		quota = &CFS_INODE(dir)->quota_infos;
	}

	mode &= ~current_umask();
	ret = cfs_meta_create(cmi->meta, dir->i_ino, &dentry->d_name, mode, uid,
			      gid, NULL, NULL, &iinfo);
	if (ret < 0) {
		cfs_log_error(cmi->log, "create dentry error %d\n", ret);
		goto out;
	}
	inode = cfs_inode_new(sb, iinfo, rdev);
	cfs_packet_inode_release(iinfo);
	if (!inode) {
		ret = -ENOMEM;
		goto out;
	}
	d_instantiate(dentry, inode);
	invalidate_iattr_cache(CFS_INODE(dir));

out:
	cfs_log_audit(cmi->log, "Mknod", dentry, NULL, ret,
		      ktime_us_delta(ktime_get(), time),
		      inode ? inode->i_ino : 0, 0);
	return ret;
}

#ifdef KERNEL_HAS_NAMESPACE
static int cfs_rename(struct user_namespace *ns, struct inode *old_dir, struct dentry *old_dentry,
		      struct inode *new_dir, struct dentry *new_dentry,
		      unsigned int flags)
#elif defined(KERNEL_HAS_RENAME_WITH_FLAGS)
static int cfs_rename(struct inode *old_dir, struct dentry *old_dentry,
		      struct inode *new_dir, struct dentry *new_dentry,
		      unsigned int flags)
#else
static int cfs_rename(struct inode *old_dir, struct dentry *old_dentry,
		      struct inode *new_dir, struct dentry *new_dentry)
#endif
{
	struct super_block *sb = old_dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	ktime_t time;
	int ret;

	/* Any flags not handled by the filesystem should result in EINVAL being returned */
#if defined(KERNEL_HAS_RENAME_WITH_FLAGS) || defined(KERNEL_HAS_NAMESPACE)
	if (flags != 0)
		return -EINVAL;
#endif

	time = ktime_get();
	ret = cfs_inode_refresh(CFS_INODE(new_dir));
	if (ret < 0)
		goto out;

	if (is_links_exceed_limit(CFS_INODE(new_dir))) {
		ret = -EDQUOT;
		goto out;
	}

	ret = cfs_meta_rename(cmi->meta, old_dir->i_ino, &old_dentry->d_name,
			      new_dir->i_ino, &new_dentry->d_name, true);
	invalidate_iattr_cache(CFS_INODE(new_dir));

out:
	cfs_log_audit(cmi->log, "Rename", old_dentry, new_dentry, ret,
		      ktime_us_delta(ktime_get(), time),
		      old_dentry->d_inode ? old_dentry->d_inode->i_ino : 0,
		      new_dentry->d_inode ? new_dentry->d_inode->i_ino : 0);
	if (ret)
		d_drop(new_dentry);
	return ret;
}

static int cfs_unlink(struct inode *dir, struct dentry *dentry)
{
	struct super_block *sb = dir->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	u64 ino = 0;
	ktime_t time;
	int ret;

	time = ktime_get();
	ret = cfs_meta_delete(cmi->meta, dir->i_ino, &dentry->d_name,
			      d_is_dir(dentry), &ino);
	invalidate_iattr_cache(CFS_INODE(dir));
	invalidate_iattr_cache(CFS_INODE(dentry->d_inode));
	cfs_log_audit(cmi->log, "Unlink", dentry, NULL, ret,
		      ktime_us_delta(ktime_get(), time), ino, 0);
	return ret;
}

/**
 * follow_link() is replaced with get_link().
 */
#ifdef KERNEL_HAS_GET_LINK
static const char *cfs_get_link(struct dentry *dentry, struct inode *inode,
				struct delayed_call *done)
{
	return inode->i_link;
}
#else
static void *cfs_follow_link(struct dentry *dentry, struct nameidata *nd)
{
	struct inode *inode = d_inode(dentry);
	struct cfs_inode *ci = (struct cfs_inode *)inode;
	struct super_block *sb = inode->i_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	cfs_log_debug(cmi->log, "dentry=" fmt_dentry "\n", pr_dentry(dentry));
	nd_set_link(nd, ci->link_target);
	return NULL;
}
#endif

static struct inode *cfs_alloc_inode(struct super_block *sb)
{
	struct cfs_inode *ci;
	struct inode *inode;

	ci = kmem_cache_alloc(inode_cache, GFP_NOFS);
	if (!ci)
		return NULL;
	inode = &ci->vfs_inode;
	if (unlikely(inode_init_always(sb, inode))) {
		kmem_cache_free(inode_cache, ci);
		return NULL;
	}
	memset(&ci->quota_infos, 0, sizeof(ci->quota_infos));
	ci->link_target = NULL;
	ci->es = 0;
	return (struct inode *)ci;
}

static void cfs_destroy_inode(struct inode *inode)
{
	struct cfs_inode *ci = (struct cfs_inode *)inode;

	if (ci->link_target)
		kfree(ci->link_target);
	cfs_extent_stream_release(ci->es);
	cfs_quota_info_array_clear(&ci->quota_infos);
	kmem_cache_free(inode_cache, ci);
}

static int cfs_drop_inode(struct inode *inode)
{
	return generic_drop_inode(inode);
}

static void cfs_put_super(struct super_block *sb)
{
	struct cfs_mount_info *cmi = sb->s_fs_info;

	cfs_log_info(cmi->log, "sb=%p{.s_fs_info=%p}\n", sb, sb->s_fs_info);
	cfs_mount_info_release(cmi);
	sb->s_fs_info = NULL;
}

static int cfs_statfs(struct dentry *dentry, struct kstatfs *kstatfs)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;
	struct cfs_volume_stat stat;
	int ret;

	ret = cfs_master_get_volume_stat(cmi->master, &stat);
	if (ret < 0) {
		cfs_log_error(cmi->log, "get volume '%s' stat error %d\n",
			      cmi->master->volume, ret);
		return ret;
	}
	memset(kstatfs, 0, sizeof(*kstatfs));
	kstatfs->f_type = CFS_FS_MAGIC;
	kstatfs->f_namelen = NAME_MAX;
	kstatfs->f_bsize = CFS_BLOCK_SIZE;
	kstatfs->f_frsize = CFS_BLOCK_SIZE;
	kstatfs->f_blocks = stat.total_size >> CFS_BLOCK_SIZE_SHIFT;
	kstatfs->f_bfree = (stat.total_size - stat.used_size) >>
			   CFS_BLOCK_SIZE_SHIFT;
	kstatfs->f_bavail = (stat.total_size - stat.used_size) >>
			    CFS_BLOCK_SIZE_SHIFT;
	kstatfs->f_files = stat.inode_count;
	kstatfs->f_ffree = CFS_INODE_MAX_ID - stat.inode_count;
	cfs_volume_stat_clear(&stat);
	return 0;
}

static int cfs_show_options(struct seq_file *seq_file, struct dentry *dentry)
{
	struct super_block *sb = dentry->d_sb;
	struct cfs_mount_info *cmi = sb->s_fs_info;

	seq_printf(seq_file, ",owner=%s", cmi->options->owner);
	seq_printf(seq_file, ",dentry_cache_valid_ms=%u",
		   cmi->options->dentry_cache_valid_ms);
	seq_printf(seq_file, ",attr_cache_valid_ms=%u",
		   cmi->options->attr_cache_valid_ms);
	seq_printf(seq_file, ",quota_cache_valid_ms=%u",
		   cmi->options->quota_cache_valid_ms);
	seq_printf(seq_file, ",enable_quota=%s",
		   cmi->options->enable_quota ? "true" : "false");
	seq_printf(seq_file, ",enable_rdma=%s",
		   cmi->options->enable_rdma ? "true" : "false");
	seq_printf(seq_file, ",rdma_port=%u", cmi->options->rdma_port);
	return 0;
}

/**
 * Filesystem
 */
static int cfs_fs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct cfs_mount_info *cmi = data;
	struct cfs_packet_inode *iinfo;
	struct inode *inode;
	int ret;

	sb->s_fs_info = cmi;
#ifdef KERNEL_HAS_SUPER_SETUP_BDI_NAME
	ret = super_setup_bdi_name(sb, "cubefs-%s", cmi->unique_name);
	if (ret < 0)
		return ret;
#else
	sb->s_bdi = &cmi->bdi;
#endif
	sb->s_blocksize = CFS_BLOCK_SIZE;
	sb->s_blocksize_bits = CFS_BLOCK_SIZE_SHIFT;
	sb->s_maxbytes = MAX_LFS_FILESIZE;
	sb->s_magic = CFS_FS_MAGIC;
	sb->s_op = &cfs_super_ops;
	sb->s_d_op = &cfs_dentry_ops;
	sb->s_time_gran = 1;
	/* no acl */
#ifdef KERNEL_HAS_SB_POSIXACL
	sb->s_flags |= SB_POSIXACL;
#else
	sb->s_flags |= MS_POSIXACL;
#endif

	ret = cfs_meta_lookup_path(cmi->meta, cmi->options->path, &iinfo);
	if (ret < 0)
		return ret;
	if (!S_ISDIR(iinfo->mode)) {
		cfs_packet_inode_release(iinfo);
		return -ENOTDIR;
	}
	/* root inode */
	inode = cfs_inode_new(sb, iinfo, 0);
	cfs_packet_inode_release(iinfo);
	if (!inode)
		return -ENOMEM;
	sb->s_root = d_make_root(inode);
	return 0;
}

/**
 * mount -t cubefs -o owner=ltptest //172.16.1.101:17010,172.16.1.102:17010,172.16.1.103:17010/ltptest /mnt/cubefs
 */
static struct dentry *cfs_mount(struct file_system_type *fs_type, int flags,
				const char *dev_str, void *opt_str)
{
	struct cfs_options *options;
	struct cfs_mount_info *cmi;
	struct dentry *dentry;

	cfs_pr_info("dev=\"%s\", options=\"%s\"\n", dev_str, (char *)opt_str);

	options = cfs_options_new(dev_str, opt_str);
	if (IS_ERR(options))
		return ERR_CAST(options);
	cmi = cfs_mount_info_new(options);
	if (IS_ERR(cmi)) {
		cfs_options_release(options);
		return ERR_CAST(cmi);
	}
	dentry = mount_nodev(fs_type, flags, cmi, cfs_fs_fill_super);
	if (IS_ERR(dentry))
		cfs_mount_info_release(cmi);
	return dentry;
}

static void cfs_kill_sb(struct super_block *sb)
{
	kill_anon_super(sb);
}

#ifdef KERNEL_HAS_FOLIO
#ifdef KERNEL_READ_PAGE
const struct address_space_operations cfs_address_ops = {
	.writepage = cfs_writepage,
	.readpage = cfs_readpage,
	.writepages = cfs_writepages,
	.dirty_folio = cfs_dirty_folio,
	.write_begin = cfs_write_begin,
	.write_end = cfs_write_end,
	.invalidate_folio = NULL,
	.releasepage = NULL,
	.direct_IO = cfs_direct_io,
};
#else
const struct address_space_operations cfs_address_ops = {
	.writepage = cfs_writepage,
	.read_folio = cfs_readfolio,
	.writepages = cfs_writepages,
	.dirty_folio = cfs_dirty_folio,
	.write_begin = cfs_write_begin,
	.write_end = cfs_write_end,
	.invalidate_folio = NULL,
	.release_folio = NULL,
	.direct_IO = cfs_direct_io,
};
#endif
#else
const struct address_space_operations cfs_address_ops = {
	.readpage = cfs_readpage,
	.readpages = cfs_readpages,
	.writepage = cfs_writepage,
	.writepages = cfs_writepages,
	.write_begin = cfs_write_begin,
	.write_end = cfs_write_end,
	.set_page_dirty = __set_page_dirty_nobuffers,
	.invalidatepage = NULL,
	.releasepage = NULL,
	.direct_IO = cfs_direct_io,
};
#endif

const struct file_operations cfs_file_fops = {
	.open = cfs_open,
	.release = cfs_release,
	.llseek = generic_file_llseek,
#ifdef KERNEL_HAS_READ_WRITE_ITER
	.read_iter = generic_file_read_iter,
	.write_iter = generic_file_write_iter,
#else
	.aio_read = generic_file_aio_read,
	.aio_write = generic_file_aio_write,
#endif
	.mmap = generic_file_mmap,
	.fsync = cfs_fsync,
	.flush = cfs_flush,
};

const struct inode_operations cfs_file_iops = {
	.permission = cfs_permission,
	.setattr = cfs_setattr,
	.getattr = cfs_getattr,
#ifdef ENABLE_XATTR
	.setxattr = cfs_setxattr,
	.getxattr = cfs_getxattr,
	.listxattr = cfs_listxattr,
	.removexattr = cfs_removexattr,
#endif
};

const struct file_operations cfs_dir_fops = {
	.open = cfs_open,
	.release = cfs_release,
	.read = generic_read_dir,
#ifdef KERNEL_HAS_ITERATE_DIR_SHARED
	.iterate_shared = cfs_iterate_dir,
#elif defined(KERNEL_HAS_ITERATE_DIR)
	.iterate = cfs_iterate_dir,
#else
	.readdir = cfs_readdir,
#endif
	.llseek = NULL,
	.fsync = noop_fsync,
};

const struct inode_operations cfs_dir_iops = {
	.lookup = cfs_lookup,
	.create = cfs_create,
	.link = cfs_link,
	.symlink = cfs_symlink,
	.mkdir = cfs_mkdir,
	.rmdir = cfs_rmdir,
	.mknod = cfs_mknod,
	.rename = cfs_rename,
	.unlink = cfs_unlink,
	.permission = cfs_permission,
	.setattr = cfs_setattr,
	.getattr = cfs_getattr,
#ifdef ENABLE_XATTR
	.setxattr = cfs_setxattr,
	.getxattr = cfs_getxattr,
	.listxattr = cfs_listxattr,
	.removexattr = cfs_removexattr,
#endif
};

const struct inode_operations cfs_symlink_iops = {
#ifdef KERNEL_HAS_GET_LINK
	.get_link = cfs_get_link,
#else
	.readlink = generic_readlink,
	.follow_link = cfs_follow_link,
#endif
};

const struct inode_operations cfs_special_iops = {
	.setattr = cfs_setattr,
	.getattr = cfs_getattr,
};

const struct dentry_operations cfs_dentry_ops = {
	.d_revalidate = cfs_d_revalidate,
};

const struct super_operations cfs_super_ops = {
	.alloc_inode = cfs_alloc_inode,
	.destroy_inode = cfs_destroy_inode,
	.drop_inode = cfs_drop_inode,
	.put_super = cfs_put_super,
	.statfs = cfs_statfs,
	.show_options = cfs_show_options,
};

struct file_system_type cfs_fs_type = {
	.name = "cubefs",
	.owner = THIS_MODULE,
	.kill_sb = cfs_kill_sb,
	.mount = cfs_mount,
};

static int proc_log_open(struct inode *inode, struct file *file)
{
#ifdef KERNEL_HAS_PDE_DATA
	file->private_data = PDE_DATA(inode);
#else
	file->private_data = pde_data(inode);
#endif
	return 0;
}

static ssize_t proc_log_read(struct file *file, char __user *buf, size_t size,
			     loff_t *ppos)
{
	struct cfs_mount_info *cmi = file->private_data;

	return cfs_log_read(cmi->log, buf, size);
}

static unsigned int proc_log_poll(struct file *file,
				  struct poll_table_struct *p)
{
	struct cfs_mount_info *cmi = file->private_data;
	struct cfs_log *log = cmi->log;

	poll_wait(file, &log->wait, p);
	if (cfs_log_size(log))
		return POLLIN | POLLRDNORM;
	return 0;
}

static int proc_log_release(struct inode *inode, struct file *file)
{
	return 0;
}

#ifdef KERNEL_HAS_PROC_OPS
static const struct proc_ops log_proc_ops = {
	.proc_open = proc_log_open,
	.proc_read = proc_log_read,
	.proc_lseek = generic_file_llseek,
	.proc_poll = proc_log_poll,
	.proc_release = proc_log_release,
};
#else
static const struct file_operations proc_log_fops = {
	.owner = THIS_MODULE,
	.open = proc_log_open,
	.read = proc_log_read,
	.llseek = generic_file_llseek,
	.poll = proc_log_poll,
	.release = proc_log_release,
};
#endif

static int init_proc(struct cfs_mount_info *cmi)
{
	char *proc_name;

	proc_name =
		kzalloc(strlen("fs/cubefs/") + strlen(cmi->unique_name) + 1,
			GFP_KERNEL);
	if (!proc_name)
		return -ENOMEM;

	sprintf(proc_name, "fs/cubefs/%s", cmi->unique_name);
	cmi->proc_dir = proc_mkdir(proc_name, NULL);
	if (!cmi->proc_dir) {
		kfree(proc_name);
		return -ENOMEM;
	}
	kfree(proc_name);

#ifdef KERNEL_HAS_PROC_OPS
	cmi->proc_log = proc_create_data("log", S_IRUSR | S_IRGRP | S_IROTH,
					 cmi->proc_dir, &log_proc_ops, cmi);
#else
	cmi->proc_log = proc_create_data("log", S_IRUSR | S_IRGRP | S_IROTH,
					 cmi->proc_dir, &proc_log_fops, cmi);
#endif
	if (!cmi->proc_log) {
		proc_remove(cmi->proc_dir);
		return -ENOMEM;
	}
	return 0;
}

static void unint_proc(struct cfs_mount_info *cmi)
{
	if (cmi->proc_log)
		proc_remove(cmi->proc_log);
	if (cmi->proc_dir)
		proc_remove(cmi->proc_dir);
}

static void update_limit_work_cb(struct work_struct *work)
{
	struct delayed_work *delayed_work = to_delayed_work(work);
	struct cfs_mount_info *cmi = container_of(
		delayed_work, struct cfs_mount_info, update_limit_work);
	struct cfs_cluster_info info;
	int ret;

	schedule_delayed_work(delayed_work,
			      msecs_to_jiffies(CFS_UPDATE_LIMIT_INTERVAL_MS));
	ret = cfs_master_get_cluster_info(cmi->master, &info);
	if (ret < 0) {
		cfs_pr_err("get cluster info error %d\n", ret);
		return;
	}
	if (info.links_limit < CFS_LINKS_MIN)
		info.links_limit = CFS_LINKS_DEFAULT;
	atomic_long_set(&cmi->links_limit, info.links_limit);
	cfs_cluster_info_clear(&info);
}

bool cfs_unique_name_exist(char *unique_name) {
	char proc_file_name[CMI_UNI_NAME_LEN];
	struct file *fp = NULL;

	sprintf(proc_file_name, "/proc/fs/cubefs/%s", unique_name);
	fp = filp_open(proc_file_name, O_RDONLY, 0);
	if (IS_ERR(fp)) {
		return false;
	}

	filp_close(fp, 0);
	return true;
}

/**
 * @return mount_info if success, error code if failed.
 */
struct cfs_mount_info *cfs_mount_info_new(struct cfs_options *options)
{
	struct cfs_mount_info *cmi;
	void *err_ptr;
#ifndef KERNEL_HAS_SUPER_SETUP_BDI_NAME
	int ret;
#endif
	size_t len = 0;
	int i = 0;

	cmi = kzalloc(sizeof(*cmi), GFP_NOFS);
	if (!cmi)
		return ERR_PTR(-ENOMEM);
	cmi->options = options;
	if (options->volume) {
		len = strlen(options->volume);
		if (len > CMI_UNI_NAME_LEN - 32) {
			len = CMI_UNI_NAME_LEN - 32;
		}
		memcpy(cmi->unique_name, options->volume, len);
		i = 0;
		do {
			sprintf(cmi->unique_name+len, "-%d", prandom_u32_max(10000));
			i++;
		} while(cfs_unique_name_exist(cmi->unique_name) && i < 10000);
		cfs_pr_info("set unique_name: %s\n", cmi->unique_name);
	} else {
		cfs_pr_err("the volume name is null\n");
		strcpy(cmi->unique_name, "null-volume");
	}

	atomic_long_set(&cmi->links_limit, CFS_LINKS_DEFAULT);
	INIT_DELAYED_WORK(&cmi->update_limit_work, update_limit_work_cb);

	cmi->log = cfs_log_new();
	if (IS_ERR(cmi->log)) {
		err_ptr = ERR_CAST(cmi->log);
		goto err_log;
	}
	if (init_proc(cmi) < 0) {
		err_ptr = ERR_PTR(-ENOMEM);
		goto err_proc;
	}
#ifndef KERNEL_HAS_SUPER_SETUP_BDI_NAME
	ret = bdi_init(&cmi->bdi);
	if (ret < 0) {
		err_ptr = ERR_PTR(ret);
		goto err_bdi;
	}
	cmi->bdi.ra_pages = (VM_MAX_READAHEAD * 1024) / PAGE_SIZE;
	ret = bdi_register(&cmi->bdi, NULL, "cubefs-%s", cmi->unique_name);
	if (ret < 0) {
		err_ptr = ERR_PTR(ret);
		goto err_bdi2;
	}
#endif
	cmi->master = cfs_master_client_new(&options->addrs, options->volume,
					    options->owner, cmi->log);
	if (IS_ERR(cmi->master)) {
		err_ptr = ERR_PTR(-ENOMEM);
		goto err_master;
	}
	cmi->meta = cfs_meta_client_new(cmi->master, options->volume, cmi->log);
	if (IS_ERR(cmi->meta)) {
		err_ptr = ERR_CAST(cmi->meta);
		goto err_meta;
	}
	cmi->ec = cfs_extent_client_new(cmi);
	if (IS_ERR(cmi->ec)) {
		err_ptr = ERR_CAST(cmi->ec);
		goto err_ec;
	}

	schedule_delayed_work(&cmi->update_limit_work, 0);
	return cmi;

err_ec:
	cfs_meta_client_release(cmi->meta);
err_meta:
	cfs_master_client_release(cmi->master);
err_master:
#ifndef KERNEL_HAS_SUPER_SETUP_BDI_NAME
	bdi_unregister(&cmi->bdi);
err_bdi2:
	bdi_destroy(&cmi->bdi);
err_bdi:
#endif
	unint_proc(cmi);
err_proc:
	cfs_log_release(cmi->log);
err_log:
	kfree(cmi);
	return err_ptr;
}

void cfs_mount_info_release(struct cfs_mount_info *cmi)
{
	if (!cmi)
		return;
	cancel_delayed_work_sync(&cmi->update_limit_work);
	cfs_extent_client_release(cmi->ec);
	cfs_meta_client_release(cmi->meta);
	cfs_master_client_release(cmi->master);
#ifndef KERNEL_HAS_SUPER_SETUP_BDI_NAME
	bdi_destroy(&cmi->bdi);
#endif
	unint_proc(cmi);
	cfs_log_release(cmi->log);
	cfs_options_release(cmi->options);
	kfree(cmi);
}

static void init_once(void *foo)
{
	struct cfs_inode *ci = (struct cfs_inode *)foo;

	inode_init_once(&ci->vfs_inode);
}

int cfs_fs_module_init(void)
{
	if (!inode_cache) {
		inode_cache = kmem_cache_create(
			"cfs_inode", sizeof(struct cfs_inode),
			__alignof__(struct cfs_inode),
			(SLAB_RECLAIM_ACCOUNT | SLAB_MEM_SPREAD), init_once);
		if (!inode_cache)
			goto oom;
	}
	if (!pagevec_cache) {
		pagevec_cache = KMEM_CACHE(cfs_page_vec, SLAB_MEM_SPREAD);
		if (!pagevec_cache)
			goto oom;
	}
	return 0;

oom:
	cfs_fs_module_exit();
	return -ENOMEM;
}

void cfs_fs_module_exit(void)
{
	if (inode_cache) {
		kmem_cache_destroy(inode_cache);
		inode_cache = NULL;
	}
	if (pagevec_cache) {
		kmem_cache_destroy(pagevec_cache);
		pagevec_cache = NULL;
	}
}
