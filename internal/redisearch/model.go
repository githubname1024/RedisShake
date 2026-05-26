package redisearch

// FieldSpec represents a single field in a RediSearch index
type FieldSpec struct {
	Name string // Field name
	Type string // TEXT, NUMERIC, TAG, GEO, VECTOR, GEOSHAPE

	// Common attributes
	Sortable    bool    // Field can be used for sorting
	Unindexed   bool    // Field is stored but not indexed
	NoStem      bool    // Don't apply stemming (TEXT fields)
	Weight      float64 // Field weight for scoring (default 1.0)
	UNF         bool    // Don't apply phonetic matching

	// TEXT field options
	PhoneticMatcher string // dm:en, dm:fr, etc.

	// NUMERIC field options
	Min float64 // Minimum value (optional)
	Max float64 // Maximum value (optional)

	// TAG field options
	Separator string // Tag separator (default: ',')
	CaseSensitive bool

	// GEO field options
	// (uses Min/Max for coordinate bounds)

	// VECTOR field options
	Algorithm       string // HNSW, FLAT
	VectorDim       int    // Dimension count
	VectorDistance  string // COSINE, L2, IP (inner product)
	VectorBlockSize int    // Block size for indexing
	M               int    // HNSW parameter
	EF              int    // HNSW parameter
	EFConstruction  int    // HNSW parameter
	EpochValue      int    // HNSW parameter
	InitialCapSize  int    // HNSW parameter

	// GEOSHAPE field options
	CoordinateSystem string // WGS84, etc.
}

// IndexOptions represents global index options
type IndexOptions struct {
	// General options
	Language     string   // Default language (default: English)
	StopWords    []string // Custom stop words (empty = default)
	SkipInitSync bool     // Skip initial sync
	Expire       int      // Document TTL in seconds
	Score        float64  // Default document score
	ScoreField   string   // Field to use for document score

	// Phonetic matching
	OnFilter     string   // Lua script for filtering

	// Optional parameters
	MaxTextFields bool   // Limit number of TEXT fields
	Temp           string // Temp rule suffix

	// Indexing behavior
	ContentField    string   // Field containing original data
	ContentFieldPos int64    // Position of content field
}

// IndexSpec represents a complete RediSearch index specification
type IndexSpec struct {
	Name    string       // Index name
	Type    string       // Index type: HASH or JSON (default: HASH)
	Prefix  []string     // Key prefixes for HASH indexes
	Filter  string       // Lua expression for document filtering
	Fields  []*FieldSpec // Field definitions
	Options *IndexOptions

	// Additional metadata
	NumDocs         int64   // Number of documents in index
	NumTerms        int64   // Number of indexed terms
	NumRecords      int64   // Number of index records
	Indexing        bool    // Is index currently indexing
	GC              bool    // Is garbage collection active
	MaxDocID        int64   // Maximum document ID
	DocTableSizeMB  float64 // Doc table memory usage
	InvertIdxSizeMB float64 // Inverted index memory usage
}

// DocumentMetadata stores metadata about a document in the index
type DocumentMetadata struct {
	DocID      string
	Score      float64
	Flags      int
	Language   string
	Payload    []byte
	SortFields map[string]interface{}
}

// InterceptedEntry represents a RediSearch entry after interception
type InterceptedEntry struct {
	IsRediSearch       bool
	Type               string // "INDEX_CREATE", "DOCUMENT_ADD", "DOCUMENT_DELETE", "NORMAL"
	OriginalEntry     *Entry
	TransformedEntries []*Entry // Entries to write to destination
}

// MigrationStats tracks RediSearch migration statistics
type MigrationStats struct {
	IndexesDetected      int64
	IndexesCreated       int64
	DocumentsProcessed   int64
	DocumentsMigrated    int64
	VectorsHandled       int64
	ErrorsEncountered    int64
	BytesMigrated        int64
	LastIndexProcessed   string
	LastDocumentID       string
}

// Entry is imported from the main RedisShake package
// This is a placeholder for documentation
type Entry struct {
	DbId           int
	Argv           []string
	CmdName        string
	Group          string
	Keys           []string
	KeyIndexes     []int
	Slots          []int
	SerializedSize int64
}
