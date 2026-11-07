#pragma once

#include <memory>
#include <Compression/CompressedSizeCalculator.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Defines.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Parsers/IAST_fwd.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

struct Settings;

/** Measures the on-disk size of a column as `MergeTreeDataPartWriterWide` writes a column of a wide part: one compressed
  * stream per substream, the codec of the column for its data substreams and only its generic codecs for the structural ones
  * (null map, array sizes, dictionary indexes), the serialization settings of the writer, and the rows written one granule
  * at a time. Used by `estimateCompressedSize`.
  */
class CompressedColumnSizeEstimator
{
public:
    /// What the writer takes from the MergeTree settings of the table and from the settings of the query.
    struct WriterSettings
    {
        static constexpr size_t MAX_COMPRESS_BLOCK_SIZE = 256ULL * 1024 * 1024;

        /// The settings of the serialization infos of a part: the versions of the serializations of `String`, `Nullable` and `Map`.
        SerializationInfoSettings serialization_info;
        /// As `MergeTreeDataPartWriterWide::getSerializationSettings`, without the stream getter.
        ISerialization::SerializeBinaryBulkSettings bulk;
        /// As `MergeTreeDataPartWriterWide::getEnumerateSettings`: the substreams of `Dynamic`, `JSON` and `Map` depend on them.
        ISerialization::EnumerateStreamsSettings enumerate;
        /// The codec of a column declared without `CODEC`, as the server picks it for a large merged part.
        CompressionCodecPtr default_codec;
        size_t index_granularity = 8192;
        size_t index_granularity_bytes = 10 * 1024 * 1024;
        size_t min_compress_block_size = DBMS_DEFAULT_BUFFER_SIZE;
        size_t max_compress_block_size = DBMS_DEFAULT_BUFFER_SIZE;
        /// The stream of a dynamic substream (a variant of `Dynamic`, a path of `JSON`) gets an adaptive buffer, whose blocks
        /// start small (`use_adaptive_write_buffer_for_dynamic_subcolumns`, `adaptive_write_buffer_initial_size`); so does every
        /// stream of a part with `min_columns_to_activate_adaptive_write_buffer` streams or more (0 never).
        bool adaptive_buffer_for_dynamic_subcolumns = true;
        size_t adaptive_buffer_min_streams = 500;
        size_t adaptive_buffer_initial_size = DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE;

        /// The settings of a merged wide part of a table with the server's MergeTree settings (the `merge_tree` section of the
        /// configuration, read from the context of the query, else from the global one) and with the settings of the query when
        /// they are known. The settings of a particular table stay unknown.
        static WriterSettings fromServer(const Settings * query_settings);
    };

    /// Writes the rows of one column of one part: the prefix, the granules, the suffix. Heap-allocated and not movable, since the
    /// stream getter holds its address.
    class Writer
    {
    public:
        ~Writer();
        Writer(const Writer &) = delete;
        Writer & operator=(const Writer &) = delete;

        /// Writes rows into the granules of the column: into the open granule first (shrunk to `granule_rows` or closed when it
        /// has more rows left than that, as the writer adjusts its last mark), then whole granules of `granule_rows` rows; the rest
        /// stays open for the next rows. A complete granule ends the compressed blocks that hold at least
        /// `min_compress_block_size` bytes, as `MergeTreeDataPartWriterWide::getCurrentMarksForColumn` does before a mark.
        /// Rows of another dynamic structure than the first block's (`Dynamic`, `JSON`) are rebuilt with it, as
        /// `MergeTreeDataPartWriterOnDisk::prepareBlockForWriting` does.
        void write(const IColumn & rows, size_t offset, size_t limit, size_t granule_rows);

        /// The suffix (the dictionary of a `LowCardinality` column, statistics) and the last compressed block of every stream;
        /// returns the compressed bytes of the column.
        UInt64 finish();

    private:
        friend class CompressedColumnSizeEstimator;
        Writer(DataTypePtr type_, SerializationPtr serialization_, const WriterSettings & settings_);

        void writePrefix(const IColumn & column);
        void writeRows(const IColumn & rows, size_t offset, size_t limit);
        void endGranule();
        CompressedSizeCalculator * calculatorFor(const ISerialization::SubstreamPath & path);
        MarkInCompressedFile markFor(const ISerialization::SubstreamPath & path);

        struct Stream
        {
            String key;
            std::unique_ptr<CompressedSizeCalculator> calculator;
        };

        DataTypePtr type;
        SerializationPtr serialization;
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        /// The structure of the first block, which every block is written with.
        ColumnPtr sample;
        /// A handful of substreams: a linear search beats hashing.
        VectorWithMemoryTracking<Stream> streams;
        /// The codecs that take the dimension of the vectors they compress (`SZ3`), set from every block as the writer sets it.
        /// Instances of this writer, since the dimension is set on the codec.
        VectorWithMemoryTracking<CompressionCodecPtr> vector_codecs;
        const size_t min_compress_block_size;
        /// The rows of the open granule, and the rows it holds when complete.
        size_t rows_in_granule = 0;
        size_t open_granule_rows = 0;
        bool finished = false;
    };

    /// Builds the layout of every substream of the type, and so validates the codec against it.
    CompressedColumnSizeEstimator(DataTypePtr type_, ASTPtr codec_description_, CompressionCodecPtr default_codec_, WriterSettings settings_);

    const DataTypePtr & getType() const { return type; }
    const WriterSettings & getSettings() const { return settings; }

    /// Rows of a granule of a block of `column`, as `computeIndexGranularity` sizes the granules of a table that has this column
    /// alone; `granuleRows(rows, bytes)` for a block of that many rows and bytes of it.
    size_t granuleRows(const IColumn & column) const;
    size_t granuleRows(size_t rows, size_t bytes) const;

    /// A writer with a compressed stream per substream of `column`, after the prefix.
    std::unique_ptr<Writer> startWriter(const IColumn & column) const;

private:
    /// The codec and the compressed block size of a substream, the initial size of its buffer when it is adaptive, and whether
    /// its serialization cuts the blocks itself by the offset of the stream (the shared data of `JSON`): then a buffer that is
    /// not adaptive has to be the whole block from the start, as the writer's (see `CompressedSizeCalculator`).
    struct StreamLayout
    {
        CompressionCodecPtr codec;
        size_t block_size = 0;
        size_t adaptive_initial_size = 0;
        bool cuts_blocks_itself = false;
    };

    StreamLayout resolveStreamLayout(const ISerialization::SubstreamPath & path) const;

    const DataTypePtr type;
    const ASTPtr codec_description;
    const CompressionCodecPtr default_codec;
    const WriterSettings settings;
    /// A substream only a column reveals (sparse offsets, the variants of `Dynamic`) is resolved when a writer starts.
    UnorderedMapWithMemoryTracking<String, StreamLayout> stream_layouts;
};

}
