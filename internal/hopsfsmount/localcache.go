// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"container/list"
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

// Global cache instance, initialized in config.go if caching is enabled
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

// Get retrieves a cached file path for the given HDFS path.
// The upstreamSize and upstreamMtime parameters are the current metadata from HopsFS,
// used to validate that the cached file hasn't been modified by another client.
// Returns the local file path and true if found and valid, or ("", false) if not cached or stale.
// If the cache entry is stale (metadata mismatch), it is automatically removed.
// Moves the entry to the front of the LRU list on successful access.
func (c *LocalCache) Get(hdfsPath string, upstreamSize int64, upstreamMtime time.Time) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[hdfsPath]
	if !ok {
		return "", false
	}

	// Validate cache entry against upstream metadata
	// If size or mtime differs, the file was modified by another client
	if entry.size != upstreamSize || !entry.mtime.Equal(upstreamMtime) {
		logger.Info("Cached staging file is stale (upstream modified), invalidating", logger.Fields{
			Path:             hdfsPath,
			TmpFile:          entry.localPath,
			"cached_size":    entry.size,
			"cached_mtime":   entry.mtime,
			"upstream_size":  upstreamSize,
			"upstream_mtime": upstreamMtime,
		})
		c.removeEntry(hdfsPath)
		return "", false
	}

	// Move to front of LRU list (most recently used)
	c.lruList.MoveToFront(entry.lruElement)

	logger.Info("Cache hit for staging file", logger.Fields{
		Path:    hdfsPath,
		TmpFile: entry.localPath,
	})

	return entry.localPath, true
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

	logger.Info("Added staging file to cache", logger.Fields{
		Path:     hdfsPath,
		TmpFile:  localPath,
		FileSize: size,
		Entries:  len(c.entries),
	})
}

// Remove explicitly removes an entry from the cache.
// This should be called when a file is deleted or renamed in DFS.
func (c *LocalCache) Remove(hdfsPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeEntry(hdfsPath)
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
	logger.Info("Evicting oldest cache entry", logger.Fields{
		Path:     entry.hdfsPath,
		TmpFile:  entry.localPath,
		FileSize: entry.size,
	})

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

	logger.Info("Cleared staging file cache", nil)
}

// Size returns the current number of entries in the cache.
func (c *LocalCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
