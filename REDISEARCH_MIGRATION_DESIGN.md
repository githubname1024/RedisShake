# RediSearch Data Migration Support - Design Document

## Overview

This document outlines a comprehensive strategy for adding **RediSearch data migration capabilities** to RedisShake. It provides analysis of both projects, architectural designs, implementation patterns, and step-by-step implementation guidance.

---

## Executive Summary

**Goal:** Extend RedisShake to recognize, extract, transform, and migrate RediSearch index structures and metadata during Redis-to-Redis data migrations.

**Key Challenge:** RediSearch data isn't just simple key-value pairs. It comprises:
- Index metadata (index specifications, field definitions)
- Indexed documents (stored as hashes or JSON)
- Internal index structures (inverted indexes, term trees, vectors)
- Document metadata (scores, rankings, TTLs)

**Solution Approach:** Create a specialized RediSearch migration module that intercepts and handles both index definition and document data.

---

## Part 1: RediSearch Architecture Analysis

### 1.1 RediSearch Data Model

#### Index Types
RediSearch supports indexing on:
- **Hashes** - Traditional Redis hash documents
- **JSON** - Redis JSON module documents (via REJSON integration)

#### Field Types
- **TEXT** - Full-text searchable fields
- **NUMERIC** - Range queryable numeric values
- **TAG** - Exact-match tags (no tokenization)
- **GEO** - Geospatial coordinates
- **VECTOR** - Vector similarity search (KNN)
- **GEOSHAPE** - Complex geospatial shapes

#### RediSearch Commands for Migration Context
The key commands for understanding migration needs are:

```
FT.CREATE <index> [ON HASH|JSON] [PREFIX <prefix> ...] <field_name> <field_type> ...
FT.ADD <index> <docid> [SCORE <score>] [LANGUAGE <language>] FIELDS <field_name> <field_value> ...
FT.SEARCH <index> <query> [LIMIT <offset> <count>]
FT.AGGREGATE <index> <expression>
FT.INFO <index>              # Shows full index specification
FT.SCAN <index> [MATCH <pattern>] [COUNT <count>]
```

### 1.2 RediSearch Storage Architecture (from code analysis)

#### Index Metadata Storage
Located in: `src/spec.c`, `src/module.c`
- Index specifications stored in Redis memory (not persistent keys by default)
- Upon reload, indexes must be recreated from definitions
- Field metadata includes: type, sortable flag, unindexed flag, weights, etc.

#### Document Storage
Located in: `src/document.c`, `src/document_add.c`
- Documents stored as Redis hashes or JSON (user data)
- Document metadata stored internally: `doc_table.c` - maintains doc IDs, scores, flags
- Inverted indexes: `src/index.c`, `src/forward_index.c`

#### Serialization
Located in: `src/rdb.c`
- RediSearch can serialize index state to RDB format
- Uses Redis module RDB save/load callbacks
- Preserves index state across persistence

### 1.3 Key RediSearch Entry Points

| File | Purpose |
|------|---------|
| `src/module-init/module-init.c` | Module initialization, command registration |
| `src/module.c` | Command handler dispatch (FT.CREATE, FT.ADD, etc.) |
| `src/spec.c` | Index specification lifecycle |
| `src/document.c` | Document indexing pipeline |
| `src/rdb.c` | RDB serialization/deserialization |

---

## Part 2: RedisShake Architecture Analysis

### 2.1 RedisShake Data Flow

```
Reader (Data Source)
    ↓
Entry (unified data model)
    ↓
Filter (filtering rules)
    ↓
Lua Functions (transformation)
    ↓
Writer (destination)
    ↓
Redis Instance (target)
```

### 2.2 Key Components

#### Readers
Location: `internal/reader/`

```go
// Interface all readers implement
type Reader interface {
    StartRead(ctx context.Context) []chan *entry.Entry
    Status() interface{}
    StatusString() string
    StatusConsistent() bool
}
```

Available readers:
- **sync_standalone_reader.go** - PSYNC protocol (streaming replication)
- **rdb_reader.go** - RDB file parsing
- **aof_reader.go** - AOF file parsing
- **scan_reader.go** - SCAN command based

#### Writers
Location: `internal/writer/`

```go
// Interface all writers implement
type Writer interface {
    Write(e *entry.Entry)
    StartWrite(ctx context.Context) (ch chan *entry.Entry)
    Close()
    Status() interface{}
}
```

Available writers:
- **redis_standalone_writer.go** - Direct Redis commands
- **redis_cluster_writer.go** - Cluster-aware routing
- **file_writer.go** - AOF/CMD/JSON export

#### Entry Model
Location: `internal/entry/entry.go`

```go
type Entry struct {
    DbId        int      // Database ID
    Argv        []string // Command arguments (e.g., ["SET", "key", "value"])
    CmdName     string   // Parsed command name
    Group       string   // Command group
    Keys        []string // Extracted key names
    KeyIndexes  []int    // Positions of keys in Argv
    Slots       []int    // Cluster slots
    SerializedSize int64
}
```

### 2.3 Filter and Function System
Location: `internal/filter/`, `cmd/redis-shake/` (Lua runtime)

- **Filter** - Rule-based filtering (key prefix, command type)
- **Lua Functions** - Custom Lua scripts can transform entries

Current pattern:
```go
// Filter runs on each entry
if !filter.Filter(e) {
    continue  // Skip this entry
}

// Lua function can transform entry
entries := luaRuntime.RunFunction(e)

// Write to destination
for _, entry := range entries {
    writer.Write(entry)
}
```

---

## Part 3: Migration Strategy

### 3.1 Challenges

1. **Index metadata isn't persisted as Redis keys** - RediSearch stores index specifications in memory
2. **Commands are order-dependent** - Must recreate indexes before adding documents
3. **Complex data structures** - Vectors, geospatial data need special handling
4. **Module detection** - Must detect if RediSearch is loaded on source
5. **Backward compatibility** - Must not break existing migration workflows

### 3.2 Solution Architecture

#### Phase 1: Index Discovery & Export
- Query `FT.INFO <index>` for all indexes
- Extract index specifications
- Store as special `__redisearch_index__` commands in the migration stream

#### Phase 2: Document Migration
- Use `FT.SCAN` to iterate all indexed documents
- For each document, get the underlying hash/JSON
- Generate appropriate commands (HSET/JSON.SET) + optionally FT.ADD for specific fields

#### Phase 3: Index Recreation
- At destination, detect `__redisearch_index__` commands
- Execute FT.CREATE with exact specifications
- Ensure documents are available before indexing

### 3.3 Implementation Approach

```
┌─────────────────────────────────────────────┐
│ RedisShake Main Flow                        │
├─────────────────────────────────────────────┤
│ 1. Read source (RDB/AOF/SYNC)              │
│                                             │
│ 2. REDISEARCH INTERCEPTOR (NEW)            │
│    a. Detect FT.* commands                 │
│    b. Extract index metadata               │
│    c. Generate metadata entries            │
│                                             │
│ 3. Process through filters                 │
│                                             │
│ 4. REDISEARCH MAPPER (NEW)                 │
│    a. Transform index commands             │
│    b. Handle field serialization           │
│                                             │
│ 5. Write to destination                    │
└─────────────────────────────────────────────┘
```

---

## Part 4: Detailed Implementation Plan

### 4.1 New Files to Create

#### A. `internal/redisearch/detector.go`
Detects RediSearch availability and indexes.

```go
package redisearch

type IndexDetector struct {
    client *redis.Redis
}

// DetectIndexes queries all available RediSearch indexes
func (d *IndexDetector) DetectIndexes(ctx context.Context) ([]*IndexSpec, error)

// GetIndexInfo retrieves FT.INFO for an index
func (d *IndexDetector) GetIndexInfo(ctx context.Context, indexName string) (*IndexSpec, error)
```

#### B. `internal/redisearch/model.go`
Data models for RediSearch structures.

```go
package redisearch

type IndexSpec struct {
    Name              string
    Prefix            []string
    Fields            []*FieldSpec
    Options           *IndexOptions
}

type FieldSpec struct {
    Name              string
    Type              string // TEXT, NUMERIC, TAG, GEO, VECTOR, GEOSHAPE
    Sortable          bool
    Unindexed         bool
    NoStem            bool
    Weight            float64
    // For VECTOR fields
    VectorDim         int
    VectorSimilarity  string
    // For NUMERIC/GEO
    Min/Max           float64
}

type IndexOptions struct {
    Language          string
    StopWords         []string
    // ... other options
}
```

#### C. `internal/redisearch/command_builder.go`
Builds Redis commands from RediSearch structures.

```go
package redisearch

// BuildCreateIndexCommand generates FT.CREATE command
func BuildCreateIndexCommand(spec *IndexSpec) []string

// BuildDocumentAddCommand generates appropriate commands for a document
func BuildDocumentAddCommand(indexName string, docID string, fields map[string]interface{}) [][]string
```

#### D. `internal/redisearch/entry_interceptor.go`
Intercepts and processes RediSearch-related entries.

```go
package redisearch

// InterceptEntry checks if entry is RediSearch-related
func InterceptEntry(e *entry.Entry) *InterceptedEntry

type InterceptedEntry struct {
    IsRediSearch      bool
    Type              string  // "INDEX_CREATE", "DOCUMENT_ADD", etc.
    OriginalEntry    *entry.Entry
    // Transformed entries for destination
    TransformedEntries []*entry.Entry
}
```

#### E. `internal/redisearch/migrator.go`
High-level migration orchestrator.

```go
package redisearch

type RediSearchMigrator struct {
    detector     *IndexDetector
    processor    *EntryProcessor
}

// PreMigrationSetup discovers indexes and generates setup entries
func (m *RediSearchMigrator) PreMigrationSetup(ctx context.Context) ([]*entry.Entry, error)

// ProcessEntry handles entries during migration
func (m *RediSearchMigrator) ProcessEntry(e *entry.Entry) []*entry.Entry
```

### 4.2 Integration Points

#### Modification 1: `cmd/redis-shake/main.go`
Add RediSearch processor to the main pipeline.

```go
// Around line 205-239 in the main loop

// After filter, before write
var processedEntries []*entry.Entry
if redisearchMigrator != nil {
    processedEntries = redisearchMigrator.ProcessEntry(e)
} else {
    processedEntries = []*entry.Entry{e}
}

for _, processedEntry := range processedEntries {
    theWriter.Write(processedEntry)
}
```

#### Modification 2: `internal/config/config.go`
Add RediSearch configuration options.

```toml
[redisearch]
enabled = true
detect_indexes = true  # Auto-detect from source
preserve_scores = true  # Keep document scores
# Handle vector fields
handle_vectors = true
vector_index_type = "HNSW"  # or others
```

#### Modification 3: `internal/writer/redis_writer.go`
Add special handling for RediSearch commands.

```go
func (w *redisWriter) Write(e *entry.Entry) {
    // Detect RediSearch index creation commands
    if e.CmdName == "FT.CREATE" {
        // Execute synchronously
        w.executeCommand(e.Argv)
        return
    }
    
    // Detect document additions requiring index first
    if e.CmdName == "FT.ADD" {
        w.ensureIndexExists(e.Argv[1])
    }
    
    // Normal write
    w.ch <- e
}
```

### 4.3 Configuration Example

New configuration section in `shake.toml`:

```toml
[redisearch]
# Enable RediSearch migration support
enabled = true

# Automatically detect and migrate indexes
auto_detect = true

# List specific indexes to migrate (if not auto-detect)
indexes = ["idx_products", "idx_users"]

# How to handle documents
document_mode = "preserve"  # preserve | recreate | recreate_only
  # preserve: Keep original hash/JSON structure
  # recreate: Recreate via FT.ADD to ensure index consistency
  # recreate_only: Only add documents that match FT.SCAN

# Vector field handling
handle_vectors = true

# Preserve document scores and metadata
preserve_metadata = true

# Exclude fields from migration
exclude_fields = []

# Field transformations (Lua script)
field_transform = ""
```

---

## Part 5: Implementation Steps

### Step 1: Foundation (Week 1)
- [ ] Create `internal/redisearch/` package
- [ ] Implement `model.go` with data structures
- [ ] Implement `detector.go` for index discovery
- [ ] Add unit tests

**Files:**
- `internal/redisearch/model.go`
- `internal/redisearch/detector.go`
- `internal/redisearch/detector_test.go`

### Step 2: Command Building (Week 2)
- [ ] Implement `command_builder.go`
- [ ] Handle all RediSearch field types
- [ ] Test command generation
- [ ] Add integration tests

**Files:**
- `internal/redisearch/command_builder.go`
- `internal/redisearch/command_builder_test.go`

### Step 3: Entry Processing (Week 3)
- [ ] Implement `entry_interceptor.go`
- [ ] Add special entry types
- [ ] Handle command transformation
- [ ] Add filter integration

**Files:**
- `internal/redisearch/entry_interceptor.go`
- `internal/redisearch/entry_interceptor_test.go`

### Step 4: Writer Integration (Week 4)
- [ ] Modify `internal/writer/redis_standalone_writer.go`
- [ ] Modify `internal/writer/redis_cluster_writer.go`
- [ ] Add index existence checking
- [ ] Add command ordering logic

**Files:**
- Modify: `internal/writer/redis_standalone_writer.go`
- Modify: `internal/writer/redis_cluster_writer.go`

### Step 5: Configuration & Orchestration (Week 5)
- [ ] Modify `internal/config/config.go`
- [ ] Implement `migrator.go`
- [ ] Add initialization in `cmd/redis-shake/main.go`
- [ ] Integration tests

**Files:**
- Modify: `internal/config/config.go`
- Modify: `cmd/redis-shake/main.go`
- `internal/redisearch/migrator.go`

### Step 6: Testing & Documentation (Week 6)
- [ ] E2E tests with RediSearch instances
- [ ] Performance testing
- [ ] Documentation updates
- [ ] Example configurations

**Files:**
- `tests/redisearch_migration_test.go`
- `docs/redisearch_migration_guide.md`
- `examples/redisearch_migration.toml`

---

## Part 6: Code Examples

### Example 1: Index Detection

```go
package redisearch

func (d *IndexDetector) DetectIndexes(ctx context.Context) ([]*IndexSpec, error) {
    // Execute FT._LIST command (or query for all via module scan)
    reply, err := d.client.DoWithStringReply("FT._LIST")
    if err != nil {
        return nil, fmt.Errorf("failed to list indexes: %w", err)
    }
    
    var indexes []*IndexSpec
    indexNames := strings.Split(reply, ",")
    
    for _, indexName := range indexNames {
        spec, err := d.GetIndexInfo(ctx, strings.TrimSpace(indexName))
        if err != nil {
            return nil, err
        }
        indexes = append(indexes, spec)
    }
    
    return indexes, nil
}

func (d *IndexDetector) GetIndexInfo(ctx context.Context, indexName string) (*IndexSpec, error) {
    // FT.INFO returns nested arrays with all index metadata
    reply := d.client.DoWithArrayReply("FT.INFO", indexName)
    
    spec := &IndexSpec{
        Name: indexName,
        Fields: make([]*FieldSpec, 0),
    }
    
    // Parse reply (complex nested structure)
    // This would need careful array/map parsing
    // See src/module.c for FT.INFO implementation
    
    return spec, nil
}
```

### Example 2: Command Building

```go
package redisearch

func BuildCreateIndexCommand(spec *IndexSpec) []string {
    cmd := []string{"FT.CREATE", spec.Name}
    
    // Add ON clause if specified
    if len(spec.Prefix) > 0 {
        cmd = append(cmd, "ON", "HASH")
        cmd = append(cmd, "PREFIX", strings.Join(spec.Prefix, " "))
    }
    
    // Add field definitions
    for _, field := range spec.Fields {
        cmd = append(cmd, field.Name, field.Type)
        
        if field.Sortable {
            cmd = append(cmd, "SORTABLE")
        }
        if field.Unindexed {
            cmd = append(cmd, "UNINDEXED")
        }
        
        // Handle type-specific options
        if field.Type == "NUMERIC" && field.Min > 0 {
            cmd = append(cmd, fmt.Sprintf("%.2f", field.Min), 
                           fmt.Sprintf("%.2f", field.Max))
        }
    }
    
    return cmd
}
```

### Example 3: Entry Interception

```go
package redisearch

func (p *EntryProcessor) ProcessEntry(e *entry.Entry) []*entry.Entry {
    if !p.enabled {
        return []*entry.Entry{e}
    }
    
    // Check if this is a RediSearch command
    cmdName := strings.ToUpper(e.CmdName)
    
    switch cmdName {
    case "FT.CREATE":
        return p.handleIndexCreate(e)
    case "FT.ADD":
        return p.handleDocumentAdd(e)
    case "FT.DEL":
        return p.handleDocumentDelete(e)
    default:
        // Regular Redis command, pass through
        return []*entry.Entry{e}
    }
}

func (p *EntryProcessor) handleIndexCreate(e *entry.Entry) []*entry.Entry {
    // e.Argv = ["FT.CREATE", "idx_name", "ON", "HASH", "field1", "TEXT", ...]
    // For now, pass through as-is
    // Later: could apply transformations (rename indexes, modify fields)
    
    return []*entry.Entry{e}
}

func (p *EntryProcessor) handleDocumentAdd(e *entry.Entry) []*entry.Entry {
    // e.Argv = ["FT.ADD", "idx_name", "docid", "FIELDS", "field1", "value1", ...]
    // May need to split into separate HSET + FT.ADD for some use cases
    
    return []*entry.Entry{e}
}
```

---

## Part 7: Testing Strategy

### Unit Tests

```go
// internal/redisearch/detector_test.go
func TestDetectIndexes(t *testing.T) {
    // Mock Redis client
    // Test index detection with various RediSearch configurations
}

// internal/redisearch/command_builder_test.go
func TestBuildCreateIndexCommand(t *testing.T) {
    spec := &IndexSpec{
        Name: "test_idx",
        Fields: []*FieldSpec{
            {Name: "title", Type: "TEXT", Sortable: true},
            {Name: "price", Type: "NUMERIC"},
        },
    }
    
    cmd := BuildCreateIndexCommand(spec)
    expected := []string{"FT.CREATE", "test_idx", "title", "TEXT", "SORTABLE", "price", "NUMERIC"}
    
    assert.Equal(t, expected, cmd)
}
```

### Integration Tests

```go
// tests/redisearch_migration_test.go
func TestRedisSearchMigration(t *testing.T) {
    // Setup: Create source Redis with RediSearch
    // - Create indexes
    // - Add documents
    
    // Execute: Run RedisShake migration
    
    // Verify: Check destination
    // - Indexes exist
    // - Documents are present
    // - Scores preserved
}
```

---

## Part 8: Handling Complex Cases

### Case 1: Vector Fields
```go
// Detect vector fields in index spec
if field.Type == "VECTOR" {
    // Options: 
    // 1. Migrate vectors as-is
    // 2. Re-generate vectors on destination
    // 3. Skip vectors (documents only)
    
    if config.SkipVectors {
        continue
    }
}
```

### Case 2: JSON Documents
```go
// RediSearch can index JSON.* paths
// Must ensure JSON module is loaded on destination
// May need to recreate FT.CREATE ON JSON syntax
```

### Case 3: Document Scores
```go
// FT.ADD accepts SCORE parameter
// Track scores from source index metadata
// Reapply on destination
```

### Case 4: Geospatial Data
```go
// GEO fields stored as lat,lon tuples
// Must preserve coordinate precision
// May need special serialization handling
```

---

## Part 9: Performance Considerations

1. **Batch Operations**: Group FT.ADD commands where possible
2. **Index Timing**: Create empty indexes first, populate asynchronously
3. **Memory**: Large vector fields may need streaming
4. **Network**: Leverage pipelining for multiple documents
5. **Monitoring**: Track RediSearch migration progress separately

---

## Part 10: Documentation

### User Guide Sections
1. **Prerequisites** - RediSearch versions supported
2. **Configuration** - All redisearch.* config options
3. **Usage Examples** - Common migration scenarios
4. **Troubleshooting** - Common issues and solutions
5. **Performance Tips** - Optimization guidelines

### Configuration Examples

```toml
# Example: Basic RediSearch migration
[redisearch]
enabled = true
auto_detect = true

# Example: Selective migration
[redisearch]
enabled = true
auto_detect = false
indexes = ["product_index"]
preserve_metadata = true

# Example: Vector optimization
[redisearch]
enabled = true
handle_vectors = true
skip_large_vectors = true
max_vector_size_mb = 100
```

---

## Part 11: Success Criteria

- [ ] All RediSearch index types can be migrated
- [ ] Document data preserved with scores/metadata
- [ ] Vector fields handled appropriately
- [ ] Backwards compatible with existing RedisShake workflows
- [ ] No performance degradation for non-RediSearch migrations
- [ ] Comprehensive test coverage (>90%)
- [ ] Complete documentation
- [ ] Works with Redis 7.x+ and Valkey 8.x+

---

## Part 12: Future Enhancements

1. **Incremental Migration** - Support checkpoint-based resume
2. **Index Optimization** - Auto-optimize indexes on destination
3. **Alias Support** - Migrate index aliases
4. **Synonym Management** - Migrate custom synonyms
5. **Stopword Lists** - Migrate custom stopwords
6. **Performance Profiling** - Built-in migration benchmarking
7. **Validation** - Pre/post migration verification

---

## Conclusion

This design provides a comprehensive roadmap for adding professional-grade RediSearch migration support to RedisShake. The modular architecture ensures minimal impact on existing functionality while providing robust handling of RediSearch's complex data structures and metadata requirements.

The phased implementation approach allows for incremental development and testing, with clear checkpoints and deliverables for each week.

