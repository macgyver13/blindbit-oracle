// Package dbpebble is a fast key-value implementation for the database.DB interface
//
// Fast initial syncs
package dbpebble

import (
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/setavenger/blindbit-lib/logging"
	"github.com/setavenger/blindbit-oracle/internal/config"
)

func OpenDB() (*pebble.DB, error) {
	dbPath := filepath.Join(config.BaseDirectory, "pebbledb", "db")

	opts := (&pebble.Options{}).EnsureDefaults()

	// CRITICAL FIX: Increase cache from 512MB to 2GB for 8.6GB dataset
	// This should improve cache hit rate from 0.09% to 85-95%
	opts.Cache = pebble.NewCache(2 << 30) // 2GB cache (was 512MB)
	logging.L.Info().Msg("PebbleDB: Configured 2GB block cache for better performance")

	// Memory & Write Performance Optimization
	opts.MemTableSize = 128 << 20        // 128MB memtables (increased from 64MB for write performance)
	opts.MemTableStopWritesThreshold = 6 // Allow more memtables before blocking writes

	// WRITE PERFORMANCE OPTIMIZATION - Sync and Compaction Settings
	opts.BytesPerSync = 16 << 20           // 16MB sync intervals (increased from 4MB) for fewer fsync calls
	opts.WALBytesPerSync = 16 << 20        // WAL sync intervals (increased for write performance)
	opts.MaxConcurrentCompactions = func() int { return 2 } // Reduce to 2 to prioritize writes over compaction

	// Level Configuration with Bloom Filters
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := range opts.Levels {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32KB blocks
		l.IndexBlockSize = 256 << 10 // 256KB index blocks
		l.FilterType = pebble.TableFilter
		l.FilterPolicy = bloom.FilterPolicy(10) // 10-bit bloom filters - FIX for 0% filter hit rate

		if i == 0 {
			l.TargetFileSize = 8 << 20 // 8MB L0 files
		} else {
			l.TargetFileSize = 32 << 20 // 32MB for other levels
		}
	}

	// WRITE PERFORMANCE: Compaction tuning - delay compactions during heavy writes
	opts.L0CompactionThreshold = 8   // Delay compaction start (increased from 4) for write performance
	opts.L0StopWritesThreshold = 50  // Allow more L0 files before stopping writes (increased from 20)

	// MAXIMUM WRITE PERFORMANCE: Conditional WAL disable based on configuration
	if config.PebbleWriteOptimized {
		opts.DisableWAL = true // ENABLED for fastest write performance during initial sync
		logging.L.Warn().Msg("PebbleDB: WAL DISABLED - Maximum write performance mode (no crash safety)")
	} else {
		opts.DisableWAL = false
		logging.L.Info().Msg("PebbleDB: WAL ENABLED - Crash safety mode")
	}

	logging.L.Info().
		Str("cache_size", "2GB").
		Str("memtable_size", "128MB").
		Str("sync_interval", "16MB").
		Bool("wal_disabled", opts.DisableWAL).
		Int("l0_compaction_threshold", 8).
		Int("l0_stop_writes_threshold", 50).
		Msg("PebbleDB: Opening with WRITE-OPTIMIZED configuration")

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, err
	}

	return db, nil
}
