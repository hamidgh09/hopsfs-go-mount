// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestLocalCachePutAndGet tests basic put and get operations
func TestLocalCachePutAndGet(t *testing.T) {
	cache := NewLocalCache(10)

	// Create a temporary file to cache
	tmpFile, err := os.CreateTemp("", "localcache_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func(name string) {
		_ = os.Remove(name)
	}(tmpFile.Name())

	testData := "test data content"
	_, _ = tmpFile.WriteString(testData)
	_ = tmpFile.Close()

	hdfsPath := "/test/file.txt"
	fileInfo, _ := os.Stat(tmpFile.Name())
	mtime := time.Now()

	// Put the file in cache
	cache.Put(hdfsPath, tmpFile.Name(), fileInfo.Size(), mtime)
	assert.Equal(t, 1, cache.Size())

	// Get with matching metadata should succeed and return correct content
	file, ok := cache.Get(hdfsPath, fileInfo.Size(), mtime)
	assert.True(t, ok)
	assert.NotNil(t, file)

	// Verify the file content matches what we wrote
	content := make([]byte, len(testData))
	n, err := file.Read(content)
	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)
	assert.Equal(t, testData, string(content))
	_ = file.Close()

	// Get with non-existent path should fail
	_, ok = cache.Get("/nonexistent/path", 100, mtime)
	assert.False(t, ok)
}

// TestLocalCacheStaleEntry tests that stale entries are invalidated
func TestLocalCacheStaleEntry(t *testing.T) {
	cache := NewLocalCache(10)

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "localcache_stale_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFilePath := tmpFile.Name()
	defer func(name string) {
		_ = os.Remove(name)
	}(tmpFilePath)

	_, _ = tmpFile.WriteString("original content")
	_ = tmpFile.Close()

	hdfsPath := "/test/stale.txt"
	cachedSize := int64(16) // "original content"
	cachedMtime := time.Now()

	// Put in cache
	cache.Put(hdfsPath, tmpFilePath, cachedSize, cachedMtime)
	assert.Equal(t, 1, cache.Size())

	// Get with different size should fail (stale)
	_, ok := cache.Get(hdfsPath, cachedSize+100, cachedMtime)
	assert.False(t, ok)
	assert.Equal(t, 0, cache.Size()) // Entry should be removed

	// Re-add for mtime test
	cache.Put(hdfsPath, tmpFilePath, cachedSize, cachedMtime)
	assert.Equal(t, 1, cache.Size())

	// Get with different mtime should fail (stale)
	differentMtime := cachedMtime.Add(time.Hour)
	_, ok = cache.Get(hdfsPath, cachedSize, differentMtime)
	assert.False(t, ok)
	assert.Equal(t, 0, cache.Size()) // Entry should be removed
}

// TestLocalCacheLRUEviction tests that LRU eviction works correctly
func TestLocalCacheLRUEviction(t *testing.T) {
	maxEntries := 3
	cache := NewLocalCache(maxEntries)

	// Create temp files for testing
	tmpFiles := make([]*os.File, maxEntries+3)
	for i := 0; i < len(tmpFiles); i++ {
		tmpFile, err := os.CreateTemp("", fmt.Sprintf("localcache_lru_%d_*", i))
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		_, _ = tmpFile.WriteString(fmt.Sprintf("content %d", i))
		_ = tmpFile.Close()
		tmpFiles[i] = tmpFile
		defer func(name string) {
			err := os.Remove(name)
			if err != nil {

			}
		}(tmpFile.Name())
	}

	mtime := time.Now()

	// Add entries up to capacity
	for i := 0; i < maxEntries; i++ {
		hdfsPath := fmt.Sprintf("/test/file%d.txt", i)
		fileInfo, _ := os.Stat(tmpFiles[i].Name())
		cache.Put(hdfsPath, tmpFiles[i].Name(), fileInfo.Size(), mtime)
	}
	assert.Equal(t, maxEntries, cache.Size())

	// Add one more entry - should evict the oldest (file0)
	fileInfo, _ := os.Stat(tmpFiles[maxEntries-1].Name())
	cache.Put("/test/file_new.txt", tmpFiles[maxEntries-1].Name(), fileInfo.Size(), mtime)
	assert.Equal(t, maxEntries, cache.Size())

	// file0 should be evicted (was the oldest)
	_, ok := cache.Get("/test/file0.txt", int64(9), mtime)
	assert.False(t, ok)

	// file1 and file2 should still be present
	file, ok := cache.Get("/test/file1.txt", int64(9), mtime)
	assert.True(t, ok)
	if file != nil {
		_ = file.Close()
	}

	file, ok = cache.Get("/test/file2.txt", int64(9), mtime)
	assert.True(t, ok)
	if file != nil {
		_ = file.Close()
	}

	// Add new entry - should evict file3 (now the oldest because file1 and file2 are accessed)
	fileInfo, _ = os.Stat(tmpFiles[maxEntries].Name())
	cache.Put("/test/file_new.txt", tmpFiles[maxEntries].Name(), fileInfo.Size(), mtime)

	// file3 should be evicted
	_, ok = cache.Get("/test/file3.txt", int64(9), mtime)
	assert.False(t, ok)

	// file1 should still exist (was accessed recently)
	file, ok = cache.Get("/test/file1.txt", int64(9), mtime)
	assert.True(t, ok)
	if file != nil {
		_ = file.Close()
	}
}

// TestLocalCacheRemove tests explicit removal of entries
func TestLocalCacheRemove(t *testing.T) {
	cache := NewLocalCache(10)

	// Create a temp file
	tmpFile, err := os.CreateTemp("", "localcache_remove_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFilePath := tmpFile.Name()
	_, _ = tmpFile.WriteString("test content")
	_ = tmpFile.Close()

	hdfsPath := "/test/remove.txt"
	mtime := time.Now()

	cache.Put(hdfsPath, tmpFilePath, 12, mtime)
	assert.Equal(t, 1, cache.Size())

	// Remove the entry
	cache.Remove(hdfsPath)
	assert.Equal(t, 0, cache.Size())

	// File should be deleted from disk
	_, err = os.Stat(tmpFilePath)
	assert.True(t, os.IsNotExist(err))

	// Removing non-existent entry should be safe
	cache.Remove("/nonexistent/path")
}

// TestLocalCacheRename tests renaming cache entries
func TestLocalCacheRename(t *testing.T) {
	cache := NewLocalCache(10)

	// Create a temp file
	tmpFile, err := os.CreateTemp("", "localcache_rename_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func(name string) {
		_ = os.Remove(name)
	}(tmpFile.Name())

	_, _ = tmpFile.WriteString("rename test")
	_ = tmpFile.Close()

	oldPath := "/test/old.txt"
	newPath := "/test/new.txt"
	mtime := time.Now()

	cache.Put(oldPath, tmpFile.Name(), 11, mtime)
	assert.Equal(t, 1, cache.Size())

	// Rename the entry
	cache.Rename(oldPath, newPath)
	assert.Equal(t, 1, cache.Size())

	// Old path should not exist
	_, ok := cache.Get(oldPath, 11, mtime)
	assert.False(t, ok)

	// New path should work
	file, ok := cache.Get(newPath, 11, mtime)
	assert.True(t, ok)
	if file != nil {
		_ = file.Close()
	}
}

// TestLocalCacheRenameWithExistingTarget tests rename when target already exists
func TestLocalCacheRenameWithExistingTarget(t *testing.T) {
	cache := NewLocalCache(10)

	// Create temp files
	tmpFile1, err := os.CreateTemp("", "localcache_rename1_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func(name string) {
		_ = os.Remove(name)
	}(tmpFile1.Name())
	_, _ = tmpFile1.WriteString("source content")
	_ = tmpFile1.Close()

	tmpFile2, err := os.CreateTemp("", "localcache_rename2_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile2Path := tmpFile2.Name()
	_, _ = tmpFile2.WriteString("target content")
	_ = tmpFile2.Close()

	sourcePath := "/test/source.txt"
	targetPath := "/test/target.txt"
	mtime := time.Now()

	// Add both entries
	cache.Put(sourcePath, tmpFile1.Name(), 14, mtime)
	cache.Put(targetPath, tmpFile2Path, 14, mtime)
	assert.Equal(t, 2, cache.Size())

	// Rename source to target - should replace target
	cache.Rename(sourcePath, targetPath)
	assert.Equal(t, 1, cache.Size())

	// Target's old file should be deleted
	_, err = os.Stat(tmpFile2Path)
	assert.True(t, os.IsNotExist(err))

	// Target path should point to source's file
	file, ok := cache.Get(targetPath, 14, mtime)
	assert.True(t, ok)
	if file != nil {
		_ = file.Close()
	}
}

// TestLocalCacheRenameNonExistent tests rename when source doesn't exist
func TestLocalCacheRenameNonExistent(t *testing.T) {
	cache := NewLocalCache(10)

	// Renaming non-existent entry should be safe (no-op)
	cache.Rename("/nonexistent/source", "/nonexistent/target")
	assert.Equal(t, 0, cache.Size())
}

// TestLocalCacheClear tests clearing all entries
func TestLocalCacheClear(t *testing.T) {
	cache := NewLocalCache(10)

	// Create temp files
	tmpFiles := make([]string, 5)
	for i := 0; i < 5; i++ {
		tmpFile, err := os.CreateTemp("", fmt.Sprintf("localcache_clear_%d_*", i))
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		_, _ = tmpFile.WriteString(fmt.Sprintf("content %d", i))
		_ = tmpFile.Close()
		tmpFiles[i] = tmpFile.Name()

		hdfsPath := fmt.Sprintf("/test/file%d.txt", i)
		cache.Put(hdfsPath, tmpFile.Name(), 9, time.Now())
	}

	assert.Equal(t, 5, cache.Size())

	// Clear cache
	cache.Clear()
	assert.Equal(t, 0, cache.Size())

	// All temp files should be deleted
	for _, path := range tmpFiles {
		_, err := os.Stat(path)
		assert.True(t, os.IsNotExist(err))
	}
}

// TestLocalCacheUpdateExisting tests updating an existing entry
func TestLocalCacheUpdateExisting(t *testing.T) {
	cache := NewLocalCache(10)

	// Create two temp files
	tmpFile1, err := os.CreateTemp("", "localcache_update1_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func(name string) {
		_ = os.Remove(name)
	}(tmpFile1.Name())
	_, _ = tmpFile1.WriteString("original")
	_ = tmpFile1.Close()

	tmpFile2, err := os.CreateTemp("", "localcache_update2_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func(name string) {
		_ = os.Remove(name)
	}(tmpFile2.Name())
	_, _ = tmpFile2.WriteString("updated")
	_ = tmpFile2.Close()

	hdfsPath := "/test/update.txt"
	mtime1 := time.Now()
	mtime2 := mtime1.Add(time.Hour)

	// Add first version
	cache.Put(hdfsPath, tmpFile1.Name(), 8, mtime1)
	assert.Equal(t, 1, cache.Size())

	// Update with second version
	cache.Put(hdfsPath, tmpFile2.Name(), 7, mtime2)
	assert.Equal(t, 1, cache.Size()) // Size should still be 1

	// Should retrieve the updated version
	file, ok := cache.Get(hdfsPath, 7, mtime2)
	assert.True(t, ok)
	if file != nil {
		_ = file.Close()
	}

	// Old metadata should not match
	_, ok = cache.Get(hdfsPath, 8, mtime1)
	assert.False(t, ok)
}

// TestLocalCacheFileNotFound tests behavior when cached file is deleted from disk
func TestLocalCacheFileNotFound(t *testing.T) {
	cache := NewLocalCache(10)

	// Create and delete a temp file
	tmpFile, err := os.CreateTemp("", "localcache_notfound_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFilePath := tmpFile.Name()
	_, _ = tmpFile.WriteString("content")
	_ = tmpFile.Close()

	hdfsPath := "/test/notfound.txt"
	mtime := time.Now()

	cache.Put(hdfsPath, tmpFilePath, 7, mtime)
	assert.Equal(t, 1, cache.Size())

	// Delete the file from disk
	_ = os.Remove(tmpFilePath)

	// Get should fail and remove the entry from cache
	_, ok := cache.Get(hdfsPath, 7, mtime)
	assert.False(t, ok)
	assert.Equal(t, 0, cache.Size())
}

// TestLocalCacheWithHopsFS tests the cache integration with actual HopsFS operations
func TestLocalCacheWithHopsFS(t *testing.T) {
	// Save original cache and restore after test
	originalCache := StagingFileCache
	defer func() {
		if StagingFileCache != nil {
			StagingFileCache.Clear()
		}
		StagingFileCache = originalCache
	}()

	// Enable local cache for this test
	StagingFileCache = NewLocalCache(5)

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		testFile := filepath.Join(mountPoint, "cache_test_file.txt")
		_ = os.Remove(testFile) // Clean up from previous runs

		testData := "Hello, this is test data for cache testing!"

		// Create a file - it should be cached when closed
		if err := createFile(testFile, testData); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		defer func() {
			_ = os.Remove(testFile)
		}()

		// File should be in cache after creation and close
		assert.Equal(t, 1, StagingFileCache.Size(), "File should be cached after creation")

		// Read the file
		content, err := os.ReadFile(testFile)
		if err != nil {
			t.Fatalf("Failed to read test file: %v", err)
		}
		assert.Equal(t, testData, string(content))

		// Modify the file with new content
		modifiedData := "Modified content!"
		if err := createFile(testFile, modifiedData); err != nil {
			t.Fatalf("Failed to modify test file: %v", err)
		}

		// Verify cache has the modified file
		hdfsPath := "/cache_test_file.txt"
		modifiedFileInfo, err := os.Stat(testFile)
		if err != nil {
			t.Fatalf("Failed to stat modified file: %v", err)
		}
		cachedFile, cacheHit := StagingFileCache.Get(hdfsPath, modifiedFileInfo.Size(), modifiedFileInfo.ModTime())
		assert.True(t, cacheHit, "Modified file should be in cache")
		if cachedFile != nil {
			_ = cachedFile.Close()
		}

		// Read again - should get the new content
		content, err = os.ReadFile(testFile)
		if err != nil {
			t.Fatalf("Failed to read modified file: %v", err)
		}
		assert.Equal(t, modifiedData, string(content))

		// Cache should still have 1 entry (updated with modified content)
		assert.Equal(t, 1, StagingFileCache.Size(), "Cache should have 1 entry after modification")
	})
}

// TestLocalCacheReadWriteAfterChown tests that reading and writing work correctly
// after chown operations when the cache is enabled, and verifies cache staleness behavior
func TestLocalCacheReadWriteAfterChown(t *testing.T) {
	// Save original cache and restore after test
	originalCache := StagingFileCache
	defer func() {
		if StagingFileCache != nil {
			StagingFileCache.Clear()
		}
		StagingFileCache = originalCache
	}()

	// Enable local cache for this test
	StagingFileCache = NewLocalCache(10)

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		testFile := filepath.Join(mountPoint, "chown_cache_test.txt")
		hdfsPath := "/chown_cache_test.txt"
		_ = os.Remove(testFile) // Clean up from previous runs

		testData := "Test data for chown cache test"

		// Create and write initial content
		if err := createFile(testFile, testData); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		defer func() {
			_ = os.Remove(testFile)
		}()

		// File is already cached after createFile() closes it
		// Record cache size and file metadata before chown
		cacheSizeBeforeChown := StagingFileCache.Size()
		t.Logf("Cache size after file creation: %d", cacheSizeBeforeChown)

		fileInfoBeforeChown, err := os.Stat(testFile)
		if err != nil {
			t.Fatalf("Failed to stat file before chown: %v", err)
		}
		mtimeBeforeChown := fileInfoBeforeChown.ModTime()

		// Get current uid/gid using syscall
		stat, ok := fileInfoBeforeChown.Sys().(*syscall.Stat_t)
		if !ok {
			t.Skip("Skipping chown test: cannot get file ownership info on this platform")
		}
		currentUID := int(stat.Uid)
		currentGID := int(stat.Gid)

		// Perform chown - set to same owner (this should work without root)
		// Note: Changing to a different user typically requires root privileges
		err = os.Chown(testFile, currentUID, currentGID)
		if err != nil {
			// Chown might fail due to permission restrictions, which is fine for this test
			t.Logf("Chown failed (may require elevated privileges): %v", err)
		}

		// Check if mtime changed after chown (some filesystems update mtime on metadata changes)
		fileInfoAfterChown, err := os.Stat(testFile)
		if err != nil {
			t.Fatalf("Failed to stat file after chown: %v", err)
		}
		mtimeAfterChown := fileInfoAfterChown.ModTime()

		if !mtimeBeforeChown.Equal(mtimeAfterChown) {
			// If mtime changed, the cache entry should become stale
			// Verify cache handles this correctly by checking if Get would fail with old mtime
			_, cacheHit := StagingFileCache.Get(hdfsPath, fileInfoBeforeChown.Size(), mtimeBeforeChown)
			assert.False(t, cacheHit, "Cache should not return entry with old mtime after chown changed mtime")
		} else {
			t.Logf("Mtime unchanged after chown (chown to same owner typically doesn't change mtime)")
		}

		// Read the file - should work regardless of cache state
		// If cache is stale, it will re-download from HopsFS
		content, err := os.ReadFile(testFile)
		if err != nil {
			t.Fatalf("Failed to read file after chown: %v", err)
		}
		assert.Equal(t, testData, string(content))

		// Write new content after chown
		newData := "Modified content after chown"
		file, err := os.OpenFile(testFile, os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			t.Fatalf("Failed to open file for writing after chown: %v", err)
		}
		_, err = file.WriteString(newData)
		if err != nil {
			_ = file.Close()
			t.Fatalf("Failed to write to file after chown: %v", err)
		}
		err = file.Close()
		if err != nil {
			t.Fatalf("Failed to close file after write: %v", err)
		}

		// Read and verify the new content
		content, err = os.ReadFile(testFile)
		if err != nil {
			t.Fatalf("Failed to read file after write: %v", err)
		}
		assert.Equal(t, newData, string(content))
	})
}

// TestLocalCacheMultipleChmodOperations tests multiple chmod operations
// with cached file reads/writes interleaved
func TestLocalCacheMultipleChmodOperations(t *testing.T) {
	// Save original cache and restore after test
	originalCache := StagingFileCache
	defer func() {
		if StagingFileCache != nil {
			StagingFileCache.Clear()
		}
		StagingFileCache = originalCache
	}()

	// Enable local cache for this test
	StagingFileCache = NewLocalCache(10)

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		testFile := filepath.Join(mountPoint, "multi_chmod_cache_test.txt")
		_ = os.Remove(testFile) // Clean up from previous runs

		testData := "Initial content"

		// Create file
		if err := createFile(testFile, testData); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		defer func() {
			_ = os.Remove(testFile)
		}()

		permissions := []os.FileMode{0644, 0755, 0600, 0444, 0644}

		for i, perm := range permissions {
			// Change permissions
			err := os.Chmod(testFile, perm)
			if err != nil {
				t.Fatalf("Failed to chmod file to %o: %v", perm, err)
			}

			// Verify permissions
			fileInfo, err := os.Stat(testFile)
			if err != nil {
				t.Fatalf("Failed to stat file after chmod to %o: %v", perm, err)
			}
			assert.Equal(t, perm, fileInfo.Mode().Perm(), "Permission mismatch at iteration %d", i)

			// Read the file
			content, err := os.ReadFile(testFile)
			if err != nil {
				t.Fatalf("Failed to read file with permission %o: %v", perm, err)
			}
			assert.Equal(t, testData, string(content), "Content mismatch at iteration %d", i)

			// Write if we have write permission (not 0444)
			if perm != 0444 {
				newData := fmt.Sprintf("Content after chmod %o", perm)
				file, err := os.OpenFile(testFile, os.O_WRONLY|os.O_TRUNC, perm)
				if err != nil {
					t.Fatalf("Failed to open file for writing with permission %o: %v", perm, err)
				}
				_, err = file.WriteString(newData)
				if err != nil {
					file.Close()
					t.Fatalf("Failed to write to file with permission %o: %v", perm, err)
				}
				err = file.Close()
				if err != nil {
					t.Fatalf("Failed to close file: %v", err)
				}
				testData = newData // Update expected content for next iteration
			}
		}
	})
}
