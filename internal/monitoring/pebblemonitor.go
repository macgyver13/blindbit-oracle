package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/setavenger/blindbit-lib/logging"
)

type PebbleMetrics struct {
	Timestamp int64 `json:"timestamp"`

	// Cache Performance
	BlockCacheHitRate float64 `json:"block_cache_hit_rate"`
	BlockCacheSize    int64   `json:"block_cache_size"`
	TableCacheHitRate float64 `json:"table_cache_hit_rate"`

	// Write Performance
	WALFiles              int     `json:"wal_files"`
	MemTableCount         int     `json:"memtable_count"`
	MemTableSize          uint64  `json:"memtable_size"`
	FlushCount            uint64  `json:"flush_count"`
	FlushBytes            uint64  `json:"flush_bytes"`
	CompactionCount       uint64  `json:"compaction_count"`
	CompactionBytes       uint64  `json:"compaction_bytes"`
	WriteAmplification    float64 `json:"write_amplification"`

	// Read Performance
	ReadAmplification float64 `json:"read_amplification"`
	IterCount         uint64  `json:"iter_count"`
	IterSeekCount     uint64  `json:"iter_seek_count"`

	// Level Stats
	LevelCount int     `json:"level_count"`
	LevelSizes []int64 `json:"level_sizes"`
	LevelFiles []int64 `json:"level_files"`

	// Disk Usage
	DiskSpaceUsage uint64 `json:"disk_space_usage"`
	LiveBytes      uint64 `json:"live_bytes"`

	// Filter Performance
	FilterHitRate float64 `json:"filter_hit_rate"`
	FilterBytes   uint64  `json:"filter_bytes"`
}

type PebbleMonitor struct {
	mu          sync.RWMutex
	db          *pebble.DB
	metrics     []PebbleMetrics
	interval    time.Duration
	maxEntries  int
	ctx         context.Context
	cancel      context.CancelFunc
	prevMetrics *pebble.Metrics
}

func NewPebbleMonitor(db *pebble.DB, interval time.Duration, maxEntries int) *PebbleMonitor {
	ctx, cancel := context.WithCancel(context.Background())
	return &PebbleMonitor{
		db:         db,
		metrics:    make([]PebbleMetrics, 0, maxEntries),
		interval:   interval,
		maxEntries: maxEntries,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (pm *PebbleMonitor) Start() {
	logging.L.Info().
		Dur("interval", pm.interval).
		Int("max_entries", pm.maxEntries).
		Msg("starting pebble monitor")
	go pm.collect()
}

func (pm *PebbleMonitor) Stop() {
	logging.L.Info().Msg("stopping pebble monitor")
	pm.cancel()
}

func (pm *PebbleMonitor) collect() {
	ticker := time.NewTicker(pm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.sample()
		}
	}
}

func (pm *PebbleMonitor) sample() {
	m := pm.db.Metrics()
	now := time.Now().Unix()

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Calculate cache hit rates
	blockCacheHitRate := 0.0
	if m.BlockCache.Hits+m.BlockCache.Misses > 0 {
		blockCacheHitRate = float64(m.BlockCache.Hits) / float64(m.BlockCache.Hits+m.BlockCache.Misses)
	}

	tableCacheHitRate := 0.0
	if m.TableCache.Hits+m.TableCache.Misses > 0 {
		tableCacheHitRate = float64(m.TableCache.Hits) / float64(m.TableCache.Hits+m.TableCache.Misses)
	}

	// Calculate write amplification using ReadAmp method
	writeAmp := float64(m.ReadAmp())
	
	// Calculate read amplification (approximate based on levels)
	readAmp := float64(m.ReadAmp())
	
	// Calculate total bytes in across all levels
	totalBytesIn := uint64(0)
	for _, level := range m.Levels {
		totalBytesIn += level.BytesIn
	}

	// Filter hit rate
	filterHitRate := 0.0
	if m.Filter.Hits+m.Filter.Misses > 0 {
		filterHitRate = float64(m.Filter.Hits) / float64(m.Filter.Hits+m.Filter.Misses)
	}

	// Collect level sizes and file counts
	levelSizes := make([]int64, len(m.Levels))
	levelFiles := make([]int64, len(m.Levels))
	for i, level := range m.Levels {
		levelSizes[i] = int64(level.Size)
		levelFiles[i] = level.NumFiles
	}

	// Calculate live bytes (approximate)
	liveBytes := uint64(0)
	for _, level := range m.Levels {
		liveBytes += uint64(level.Size)
	}

	metrics := PebbleMetrics{
		Timestamp:            now,
		BlockCacheHitRate:    blockCacheHitRate,
		BlockCacheSize:       int64(m.BlockCache.Size),
		TableCacheHitRate:    tableCacheHitRate,
		WALFiles:             int(m.WAL.Files),
		MemTableCount:        int(m.MemTable.Count),
		MemTableSize:         m.MemTable.Size,
		FlushCount:           uint64(m.Flush.Count),
		FlushBytes:           m.Flush.AsIngestBytes, // Use AsIngestBytes as proxy for flush bytes
		CompactionCount:      uint64(m.Compact.Count),
		CompactionBytes:      m.Compact.EstimatedDebt, // Use EstimatedDebt as proxy
		WriteAmplification:   writeAmp,
		ReadAmplification:    readAmp,
		IterCount:            uint64(m.TableIters),
		LevelCount:           len(m.Levels),
		LevelSizes:           levelSizes,
		LevelFiles:           levelFiles,
		DiskSpaceUsage:       m.DiskSpaceUsage(),
		LiveBytes:            liveBytes,
		FilterHitRate:        filterHitRate,
		FilterBytes:          0, // Filter doesn't have Size field, set to 0
	}

	pm.metrics = append(pm.metrics, metrics)

	// Keep only recent entries
	if len(pm.metrics) > pm.maxEntries {
		pm.metrics = pm.metrics[len(pm.metrics)-pm.maxEntries:]
	}

	pm.prevMetrics = m

	// Log key metrics periodically
	if len(pm.metrics)%10 == 0 { // Every 10 samples
		logging.L.Debug().
			Float64("block_cache_hit_rate", blockCacheHitRate).
			Float64("write_amplification", writeAmp).
			Int("memtable_count", int(m.MemTable.Count)).
			Uint64("memtable_size_mb", m.MemTable.Size/(1024*1024)).
			Int64("l0_files", levelFiles[0]).
			Uint64("disk_space_mb", m.DiskSpaceUsage()/(1024*1024)).
			Msg("pebble_metrics_sample")
	}
}

func (pm *PebbleMonitor) GetLatestMetrics() *PebbleMetrics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if len(pm.metrics) == 0 {
		return nil
	}
	return &pm.metrics[len(pm.metrics)-1]
}

func (pm *PebbleMonitor) GetAllMetrics() []PebbleMetrics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return append([]PebbleMetrics(nil), pm.metrics...)
}

func (pm *PebbleMonitor) ExportJSON(filename string) error {
	metrics := pm.GetAllMetrics()
	data, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

// Performance analysis helpers
func (pm *PebbleMonitor) DetectBottlenecks() []string {
	latest := pm.GetLatestMetrics()
	if latest == nil {
		return nil
	}

	var bottlenecks []string

	if latest.BlockCacheHitRate < 0.8 {
		bottlenecks = append(bottlenecks, fmt.Sprintf("Low block cache hit rate: %.2f%%", latest.BlockCacheHitRate*100))
	}

	if latest.WriteAmplification > 10 {
		bottlenecks = append(bottlenecks, fmt.Sprintf("High write amplification: %.2f", latest.WriteAmplification))
	}

	if latest.MemTableCount > 4 {
		bottlenecks = append(bottlenecks, fmt.Sprintf("High memtable count: %d", latest.MemTableCount))
	}

	if len(latest.LevelFiles) > 0 && latest.LevelFiles[0] > 100 {
		bottlenecks = append(bottlenecks, fmt.Sprintf("High L0 file count: %d", latest.LevelFiles[0]))
	}

	if latest.TableCacheHitRate < 0.9 {
		bottlenecks = append(bottlenecks, fmt.Sprintf("Low table cache hit rate: %.2f%%", latest.TableCacheHitRate*100))
	}

	if latest.FilterHitRate < 0.8 {
		bottlenecks = append(bottlenecks, fmt.Sprintf("Low filter hit rate: %.2f%%", latest.FilterHitRate*100))
	}

	return bottlenecks
}

// GetSummary returns a human-readable summary of the latest metrics
func (pm *PebbleMonitor) GetSummary() string {
	latest := pm.GetLatestMetrics()
	if latest == nil {
		return "No metrics available"
	}

	return fmt.Sprintf(`PebbleDB Performance Summary:
  Block Cache Hit Rate: %.2f%%
  Write Amplification: %.2f
  Memtable Count: %d
  Memtable Size: %.1f MB
  L0 Files: %d
  Disk Usage: %.1f MB
  Filter Hit Rate: %.2f%%`,
		latest.BlockCacheHitRate*100,
		latest.WriteAmplification,
		latest.MemTableCount,
		float64(latest.MemTableSize)/(1024*1024),
		latest.LevelFiles[0],
		float64(latest.DiskSpaceUsage)/(1024*1024),
		latest.FilterHitRate*100)
}