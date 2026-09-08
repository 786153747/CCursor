/**
 * Blob Store — 进程内热缓存 (客户端是 blob 的唯一持久持有方)
 *
 * 对齐官方架构: 服务端不落盘任何对话 blob。这里只是当前进程的热缓存:
 *   - 本 run 刚发给客户端 (setBlobArgs) 的 blob 立即可读, 同轮压缩 / 重建不必回取;
 *   - 进程未重启时上一轮的 blob 直接命中, 省一次回取;
 *   - 未命中 (重启 / 淘汰) → 调用方经 getBlobArgs 向客户端取 (clientBlobFetch)。
 *
 * 值是 base64 文本 (JSON blob = base64(JSON), 二进制 blob = base64(raw bytes))。
 * 按字节 LRU 淘汰, 读写都刷新新鲜度。被淘汰的 blob 总能从客户端取回, 上限只需远大于
 * 单个会话的工作集 (实测最大会话 ~4 MB / 1.1k blob), 128 MB 留出并发子代理的余量。
 */
const DEFAULT_MAX_BYTES = 128 * 1024 * 1024

/** blobId → blobData (base64); Map 的插入顺序即 LRU 顺序, 最旧在前 */
const blobCache = new Map<string, string>()
let cachedBytes = 0
let maxBytes = DEFAULT_MAX_BYTES

export function cacheBlob(blobId: string, blobData: string): void {
    const previous = blobCache.get(blobId)
    if (previous !== undefined) {
        cachedBytes -= previous.length
        blobCache.delete(blobId)
    }
    blobCache.set(blobId, blobData)
    cachedBytes += blobData.length
    evictLeastRecentlyUsed()
}

export function getCachedBlob(blobId: string): string | undefined {
    const blobData = blobCache.get(blobId)
    if (blobData === undefined)
        return undefined
    // 删掉重插 = 移到 Map 末尾 (最新)
    blobCache.delete(blobId)
    blobCache.set(blobId, blobData)
    return blobData
}

function evictLeastRecentlyUsed(): void {
    for (const [blobId, blobData] of blobCache) {
        if (cachedBytes <= maxBytes || blobCache.size <= 1)
            return
        blobCache.delete(blobId)
        cachedBytes -= blobData.length
    }
}

export function getBlobCacheStats(): { entries: number, bytes: number } {
    return { entries: blobCache.size, bytes: cachedBytes }
}

export function resetBlobCacheForTests(options: { maxBytes?: number } = {}): void {
    blobCache.clear()
    cachedBytes = 0
    maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES
}
