// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"container/list"
	"fmt"
	"os"
	"sync"
	"time"

	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// LocalCache manages cached staging files using LRU eviction.
// When files are written and closed, their local staging copies are kept
// in this cache for faster reopening instead of downloading from DFS again.
type LocalCache struct {
	mu         sync.Mutex
	maxEntries int
	entries    map[string]*CacheEntry
	lruList    *list.List // front = most recently used, back = least recently used
}

// CacheEntry represents a cached staging file
type CacheEntry struct {
	hdfsPath   string
	localPath  string
	size       int64
	mtime      time.Time // modification time when cached, used to detect upstream changes
	lruElement *list.Element
}

// StagingFileCache Local cache instance, initialized in config.go if caching is enabled
var StagingFileCache *LocalCache

// NewLocalCache creates a new cache with the given maximum number of entries.
// When the cache is full, the least recently used entry is evicted.
func NewLocalCache(maxEntries int) *LocalCache {
	return &LocalCache{
		maxEntries: maxEntries,
		entries:    make(map[string]*CacheEntry),
		lruList:    list.New(),
	}
}

// Get retrieves a cached file for the given HDFS path.
// The upstreamSize and upstreamMtime parameters are the current metadata from HopsFS,
// used to validate that the cached file hasn't been modified by another client.
// Returns an open file handle and true if found and valid, or (nil, false) if not cached or stale.
// If the cache entry is stale (metadata mismatch) or file can't be opened, it is automatically removed.
// Moves the entry to the front of the LRU list on successful access.
func (c *LocalCache) Get(hdfsPath string, upstreamSize int64, upstreamMtime time.Time) (*os.File, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[hdfsPath]
	if !ok {
		logger.Debug("Cache miss for staging file", logger.Fields{
			Path: hdfsPath,
		})
		return nil, false
	}

	// Validate cache entry against upstream metadata
	// If size or mtime differs, the file was modified by another client
	if entry.size != upstreamSize || !entry.mtime.Equal(upstreamMtime) {
		logger.Debug(fmt.Sprintf("Cached staging file is stale, invalidating. cached[size=%d, mtime=%v] upstream[size=%d, mtime=%v]",
			entry.size, entry.mtime, upstreamSize, upstreamMtime), logger.Fields{
			Path:    hdfsPath,
			TmpFile: entry.localPath,
		})
		c.removeEntry(hdfsPath)
		return nil, false
	}

	// Try to open the cached file
	localFile, err := os.OpenFile(entry.localPath, os.O_RDWR, 0600)
	if err != nil {
		logger.Warn("Failed to open cached staging file, removing from cache", logger.Fields{
			Path:    hdfsPath,
			TmpFile: entry.localPath,
			Error:   err,
		})
		c.removeEntry(hdfsPath)
		return nil, false
	}

	// Move to front of LRU list (most recently used)
	c.lruList.MoveToFront(entry.lruElement)

	logger.Debug("Cache hit for staging file", logger.Fields{
		Path:    hdfsPath,
		TmpFile: entry.localPath,
	})

	return localFile, true
}

// Put adds a staging file to the cache. If the cache is full, the least
// recently used entry is evicted first. If an entry already exists for
// this path, it is updated and moved to the front of the LRU list.
// The mtime parameter should be the modification time from HopsFS, used
// to detect if the file was modified by another client.
func (c *LocalCache) Put(hdfsPath string, localPath string, size int64, mtime time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if entry already exists
	if existing, ok := c.entries[hdfsPath]; ok {
		// Update existing entry
		existing.localPath = localPath
		existing.size = size
		existing.mtime = mtime
		c.lruList.MoveToFront(existing.lruElement)
		logger.Debug("Updated existing cache entry", logger.Fields{
			Path:     hdfsPath,
			TmpFile:  localPath,
			FileSize: size,
		})
		return
	}

	// Evict oldest entries if cache is full
	for len(c.entries) >= c.maxEntries {
		c.evictOldest()
	}

	// Create new entry
	entry := &CacheEntry{
		hdfsPath:  hdfsPath,
		localPath: localPath,
		size:      size,
		mtime:     mtime,
	}
	entry.lruElement = c.lruList.PushFront(entry)
	c.entries[hdfsPath] = entry

	logger.Debug("Added staging file to cache", logger.Fields{
		Path:     hdfsPath,
		TmpFile:  localPath,
		FileSize: size,
		Entries:  len(c.entries),
	})
}

// Remove explicitly removes an entry from the cache.
// This should be called when a file is deleted in DFS.
func (c *LocalCache) Remove(hdfsPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeEntry(hdfsPath)
}

// Rename transfers a cache entry from oldPath to newPath.
// If the entry doesn't exist for oldPath, this is a no-op.
// If an entry already exists for newPath, it is replaced.
func (c *LocalCache) Rename(oldPath, newPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[oldPath]
	if !ok {
		// No cache entry for old path, nothing to transfer
		logger.Debug("Cache rename: no entry for old path", logger.Fields{
			From: oldPath,
			To:   newPath,
		})
		return
	}

	// Remove any existing entry at the new path
	if _, exists := c.entries[newPath]; exists {
		c.removeEntry(newPath)
	}

	// Update the entry's hdfsPath and move to new key
	delete(c.entries, oldPath)
	entry.hdfsPath = newPath
	c.entries[newPath] = entry

	// Move to front of LRU (most recently used)
	c.lruList.MoveToFront(entry.lruElement)

	logger.Debug("Cache entry renamed", logger.Fields{
		From:    oldPath,
		To:      newPath,
		TmpFile: entry.localPath,
	})
}

// removeEntry removes an entry without locking (internal use only)
func (c *LocalCache) removeEntry(hdfsPath string) {
	entry, ok := c.entries[hdfsPath]
	if !ok {
		return
	}

	// Remove from LRU list
	c.lruList.Remove(entry.lruElement)

	// Delete local file
	if err := os.Remove(entry.localPath); err != nil && !os.IsNotExist(err) {
		logger.Warn("Failed to remove cached staging file", logger.Fields{
			Path:    hdfsPath,
			TmpFile: entry.localPath,
			Error:   err,
		})
	}

	// Remove from map
	delete(c.entries, hdfsPath)

	logger.Debug("Removed staging file from cache", logger.Fields{
		Path:    hdfsPath,
		TmpFile: entry.localPath,
	})
}

// evictOldest removes the least recently used entry from the cache.
// Must be called with mutex held.
func (c *LocalCache) evictOldest() {
	oldest := c.lruList.Back()
	if oldest == nil {
		return
	}

	entry := oldest.Value.(*CacheEntry)
	c.removeEntry(entry.hdfsPath)
}

// Clear removes all entries from the cache.
// This should be called during shutdown.
func (c *LocalCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for hdfsPath := range c.entries {
		c.removeEntry(hdfsPath)
	}

	logger.Debug("Cleared staging file cache", nil)
}

// Size returns the current number of entries in the cache.
func (c *LocalCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// ShouldCache returns true if a file should be downloaded to the local cache.
// Checks disk space and file size limits.
func (c *LocalCache) ShouldCache(file *FileINode) bool {
	// Check if file exceeds max cacheable size
	if LocalCacheMaxFileSize > 0 && int64(file.Attrs.Size) > LocalCacheMaxFileSize {
		logger.Debug("File too large for caching", logger.Fields{
			Path:     file.AbsolutePath(),
			FileSize: file.Attrs.Size,
		})
		return false
	}

	if err := file.checkDiskSpace(); err != nil {
		logger.Warn("Not enough disk space for caching", logger.Fields{
			Path:  file.AbsolutePath(),
			Error: err,
		})
		return false
	}
	return true
}

// GetOrLoad tries to get a file from cache, or downloads it to cache if not found.
// Returns a FileProxy if successful (either from cache or freshly downloaded), or nil if caching is not possible.
func (c *LocalCache) GetOrLoad(file *FileINode, hdfsAccessor HdfsAccessor) FileProxy {
	absPath := file.AbsolutePath()

	upstreamInfo, err := hdfsAccessor.Stat(absPath)
	if err != nil {
		logger.Warn("Failed to stat file for cache validation, skipping cache", logger.Fields{
			Path:  absPath,
			Error: err,
		})
		return nil
	}

	// Update file.Attrs with upstream metadata so closeStaging can use correct mtime for caching
	file.Attrs.Size = upstreamInfo.Size
	file.Attrs.Mtime = upstreamInfo.Mtime

	if cachedFile, ok := c.Get(absPath, int64(upstreamInfo.Size), upstreamInfo.Mtime); ok {
		return &LocalRWFileProxy{localFile: cachedFile, file: file}
	}

	if !c.ShouldCache(file) {
		return nil
	}

	// Download to staging file
	return file.createStagingFileForRead()
}
