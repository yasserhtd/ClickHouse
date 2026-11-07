#include <AggregateFunctions/CompressedColumnSizeEstimator.h>

#include <algorithm>
#include <limits>
#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Formats/MarkInCompressedFile.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 low_cardinality_max_dictionary_size;
    extern const SettingsBool low_cardinality_use_single_dictionary_for_part;
    extern const SettingsUInt64 max_compress_block_size;
    extern const SettingsBool merge_tree_use_v1_object_and_dynamic_serialization;
    extern const SettingsUInt64 min_compress_block_size;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsNonZeroUInt64 adaptive_write_buffer_initial_size;
    extern const MergeTreeSettingsBool compute_exact_num_defaults_for_sparse_columns;
    extern const MergeTreeSettingsString default_compression_codec;
    extern const MergeTreeSettingsMergeTreeDynamicSerializationVersion dynamic_serialization_version;
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsFloat map_buckets_coefficient;
    extern const MergeTreeSettingsUInt64 map_buckets_min_avg_size;
    extern const MergeTreeSettingsMergeTreeMapBucketsStrategy map_buckets_strategy;
    extern const MergeTreeSettingsMergeTreeMapSerializationVersion map_serialization_version;
    extern const MergeTreeSettingsNonZeroUInt64 max_buckets_in_map;
    extern const MergeTreeSettingsUInt64 max_compress_block_size;
    extern const MergeTreeSettingsUInt64 min_compress_block_size;
    extern const MergeTreeSettingsMergeTreeNullableSerializationVersion nullable_serialization_version;
    extern const MergeTreeSettingsMergeTreeObjectSerializationVersion object_serialization_version;
    extern const MergeTreeSettingsNonZeroUInt64 object_shared_data_buckets_for_wide_part;
    extern const MergeTreeSettingsMergeTreeObjectSharedDataSerializationVersion object_shared_data_serialization_version;
    extern const MergeTreeSettingsNonZeroUInt64 object_shared_data_target_chunk_rows;
    extern const MergeTreeSettingsMergeTreeObjectTypedSharedDataSerializationVersion object_typed_shared_data_serialization_version;
    extern const MergeTreeSettingsBool propagate_types_serialization_versions_to_nested_types;
    extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
    extern const MergeTreeSettingsMergeTreeStringSerializationVersion string_serialization_version;
    extern const MergeTreeSettingsBool use_adaptive_write_buffer_for_dynamic_subcolumns;
    extern const MergeTreeSettingsBool use_compact_variant_discriminators_serialization;
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

CompressedColumnSizeEstimator::WriterSettings CompressedColumnSizeEstimator::WriterSettings::fromServer(const Settings * query_settings)
{
    /// The context is not kept: the function may outlive the query, as part of an `AggregateFunction(...)` data type.
    ContextPtr context;
    if (CurrentThread::isInitialized())
        context = CurrentThread::get().tryGetQueryContext();
    if (!context)
        context = Context::getGlobalContextInstance();

    static const MergeTreeSettings default_merge_tree_settings;
    static const Settings default_query_settings;
    const MergeTreeSettings & merge_tree = context ? context->getMergeTreeSettings() : default_merge_tree_settings;
    const Settings & query = query_settings ? *query_settings : default_query_settings;

    WriterSettings settings;

    /// As the serialization infos of a part are set up by `MergeTreeDataWriter` and `MergeTask`. The ratio of defaults would only
    /// gate the sparse serialization, and the kind comes from the column: a column that is stored sparse arrives sparse.
    settings.serialization_info = SerializationInfoSettings(
        /*ratio_of_defaults_for_sparse_=*/ 0.0,
        /*choose_kind_=*/ false,
        merge_tree[MergeTreeSetting::compute_exact_num_defaults_for_sparse_columns],
        merge_tree[MergeTreeSetting::serialization_info_version],
        merge_tree[MergeTreeSetting::string_serialization_version],
        merge_tree[MergeTreeSetting::nullable_serialization_version],
        merge_tree[MergeTreeSetting::map_serialization_version],
        merge_tree[MergeTreeSetting::propagate_types_serialization_versions_to_nested_types]);

    /// As `MergeTreeWriterSettings` and `MergeTreeDataPartWriterWide::getSerializationSettings`, for a merged wide part.
    const bool v1_object_and_dynamic = query[Setting::merge_tree_use_v1_object_and_dynamic_serialization];
    auto & bulk = settings.bulk;
    bulk.data_part_type = MergeTreeDataPartType::Wide;
    bulk.low_cardinality_max_dictionary_size = query[Setting::low_cardinality_max_dictionary_size];
    bulk.low_cardinality_use_single_dictionary_for_part = query[Setting::low_cardinality_use_single_dictionary_for_part];
    bulk.use_compact_variant_discriminators_serialization = merge_tree[MergeTreeSetting::use_compact_variant_discriminators_serialization];
    bulk.dynamic_serialization_version
        = v1_object_and_dynamic ? MergeTreeDynamicSerializationVersion::V1 : merge_tree[MergeTreeSetting::dynamic_serialization_version];
    bulk.object_serialization_version
        = v1_object_and_dynamic ? MergeTreeObjectSerializationVersion::V1 : merge_tree[MergeTreeSetting::object_serialization_version];
    bulk.object_shared_data_serialization_version = merge_tree[MergeTreeSetting::object_shared_data_serialization_version];
    bulk.object_shared_data_buckets = merge_tree[MergeTreeSetting::object_shared_data_buckets_for_wide_part];
    bulk.object_shared_data_target_chunk_rows = merge_tree[MergeTreeSetting::object_shared_data_target_chunk_rows];
    bulk.object_typed_shared_data_serialization_version = merge_tree[MergeTreeSetting::object_typed_shared_data_serialization_version];
    bulk.max_buckets_in_map = merge_tree[MergeTreeSetting::max_buckets_in_map];
    bulk.map_buckets_strategy = merge_tree[MergeTreeSetting::map_buckets_strategy];
    bulk.map_buckets_coefficient = static_cast<double>(merge_tree[MergeTreeSetting::map_buckets_coefficient]);
    bulk.map_buckets_min_avg_size = merge_tree[MergeTreeSetting::map_buckets_min_avg_size];
    bulk.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::SUFFIX;

    /// As `MergeTreeDataPartWriterWide::getEnumerateSettings`.
    auto & enumerate = settings.enumerate;
    enumerate.object_serialization_version = bulk.object_serialization_version;
    enumerate.object_shared_data_serialization_version = bulk.object_shared_data_serialization_version;
    enumerate.object_shared_data_buckets = bulk.object_shared_data_buckets;
    enumerate.object_shared_data_target_chunk_rows = bulk.object_shared_data_target_chunk_rows;
    enumerate.max_buckets_in_map = bulk.max_buckets_in_map;
    enumerate.map_buckets_strategy = bulk.map_buckets_strategy;
    enumerate.map_buckets_coefficient = bulk.map_buckets_coefficient;
    enumerate.map_buckets_min_avg_size = bulk.map_buckets_min_avg_size;
    enumerate.data_part_type = MergeTreeDataPartType::Wide;

    /// As `MergeTreeData::getCompressionCodecForPart` for a large merged part, where most of the data of a table ends up: the
    /// `default_compression_codec` setting, else the `compression` section of the configuration, else the default codec.
    const String & codec_setting = merge_tree[MergeTreeSetting::default_compression_codec].value;
    if (!codec_setting.empty())
        settings.default_codec = CompressionCodecFactory::instance().get(codec_setting);
    else if (context)
        settings.default_codec = context->chooseCompressionCodec(std::numeric_limits<size_t>::max(), /*part_size_ratio=*/ 1.0);
    else
        settings.default_codec = CompressionCodecFactory::instance().getDefaultCodec();

    settings.index_granularity = merge_tree[MergeTreeSetting::index_granularity];
    settings.index_granularity_bytes = merge_tree[MergeTreeSetting::index_granularity_bytes];

    /// The MergeTree setting when it is set, else the setting of the query, as `MergeTreeWriterSettings` picks them.
    const UInt64 merge_tree_min = merge_tree[MergeTreeSetting::min_compress_block_size];
    const UInt64 merge_tree_max = merge_tree[MergeTreeSetting::max_compress_block_size];
    settings.min_compress_block_size
        = std::min<size_t>(merge_tree_min ? merge_tree_min : query[Setting::min_compress_block_size], MAX_COMPRESS_BLOCK_SIZE);
    settings.max_compress_block_size
        = std::min<size_t>(merge_tree_max ? merge_tree_max : query[Setting::max_compress_block_size], MAX_COMPRESS_BLOCK_SIZE);
    /// The serializations that end compressed blocks themselves (the shared data of `JSON`) do so from this size on.
    bulk.min_compress_block_size = settings.min_compress_block_size;

    /// As `MergeTreeDataPartWriterWide::addStreams`. A part with `min_columns_to_activate_adaptive_write_buffer` streams gives
    /// every stream an adaptive buffer, which a single column cannot tell.
    settings.adaptive_buffer_for_dynamic_subcolumns = merge_tree[MergeTreeSetting::use_adaptive_write_buffer_for_dynamic_subcolumns];
    settings.adaptive_buffer_initial_size = merge_tree[MergeTreeSetting::adaptive_write_buffer_initial_size];

    return settings;
}

CompressedColumnSizeEstimator::Writer::Writer(DataTypePtr type_, SerializationPtr serialization_, const WriterSettings & settings_)
    : type(std::move(type_))
    , serialization(std::move(serialization_))
    , settings(settings_.bulk)
    , min_compress_block_size(settings_.min_compress_block_size)
{
}

CompressedColumnSizeEstimator::Writer::~Writer()
{
    /// Left over by an exception; the finished ones are gone already.
    for (auto & stream : streams)
        stream.calculator->cancel();
}

void CompressedColumnSizeEstimator::Writer::writePrefix(const IColumn & column)
{
    if (column.hasDynamicStructure())
    {
        auto first = column.cloneEmpty();
        first->takeExactDynamicStructureFrom(column);
        sample = std::move(first);
    }
    serialization->serializeBinaryBulkStatePrefix(column, settings, state);
}

CompressedSizeCalculator * CompressedColumnSizeEstimator::Writer::calculatorFor(const ISerialization::SubstreamPath & path)
{
    const String key = ISerialization::getSubstreamsCacheKeyForStream(path);
    for (auto & stream : streams)
        if (stream.key == key)
            return stream.calculator.get();

    /// The writer has no stream for an ephemeral subcolumn either.
    if (ISerialization::isEphemeralSubcolumn(path, path.size()))
        return nullptr;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "No stream for substream {}", ISerialization::getSubcolumnNameForStream(path));
}

MarkInCompressedFile CompressedColumnSizeEstimator::Writer::markFor(const ISerialization::SubstreamPath & path)
{
    /// As the writer's mark: the position in the compressed file and in the open block. The serializations that write marks
    /// themselves (the shared data of `JSON`) store these, so their value matters for the compressed size.
    if (const auto * calculator = calculatorFor(path))
        return {calculator->getCompressedBytesOfEndedBlocks(), calculator->blockBytes()};
    return {};
}

void CompressedColumnSizeEstimator::Writer::write(const IColumn & rows, size_t offset, size_t limit, size_t granule_rows)
{
    /// As `MergeTreeDataPartWriterWide::writeColumn` before the granules of a block.
    for (const auto & codec : vector_codecs)
        MergeTreeDataPartWriterOnDisk::setVectorDimensionsIfNeeded(codec, &rows);

    for (size_t end = offset + limit; offset < end;)
    {
        if (rows_in_granule == 0)
            open_granule_rows = granule_rows;

        const size_t count = std::min(open_granule_rows - rows_in_granule, end - offset);
        writeRows(rows, offset, count);
        offset += count;
        rows_in_granule += count;

        if (rows_in_granule >= open_granule_rows)
        {
            endGranule();
            rows_in_granule = 0;
        }
    }
}

void CompressedColumnSizeEstimator::Writer::writeRows(const IColumn & rows, size_t offset, size_t limit)
{
    if (sample && !rows.dynamicStructureEquals(*sample))
    {
        auto rebuilt = type->createColumn();
        rebuilt->takeExactDynamicStructureFrom(*sample);
        rebuilt->insertRangeFrom(rows, offset, limit);
        serialization->serializeBinaryBulkWithMultipleStreams(*rebuilt, 0, limit, settings, state);
    }
    else
        serialization->serializeBinaryBulkWithMultipleStreams(rows, offset, limit, settings, state);
}

void CompressedColumnSizeEstimator::Writer::endGranule()
{
    for (auto & stream : streams)
        if (stream.calculator->blockBytes() >= min_compress_block_size)
            stream.calculator->endBlock();
}

UInt64 CompressedColumnSizeEstimator::Writer::finish()
{
    chassert(!finished);
    finished = true;

    serialization->serializeBinaryBulkStateSuffix(settings, state);
    state.reset();

    UInt64 compressed = 0;
    for (auto & stream : streams)
    {
        stream.calculator->finalize();
        compressed += stream.calculator->getCompressedBytes();
    }
    streams.clear();
    return compressed;
}

CompressedColumnSizeEstimator::CompressedColumnSizeEstimator(
    DataTypePtr type_, ASTPtr codec_description_, CompressionCodecPtr default_codec_, WriterSettings settings_)
    : type(std::move(type_))
    , codec_description(std::move(codec_description_))
    , default_codec(default_codec_ ? std::move(default_codec_) : CompressionCodecFactory::instance().getDefaultCodec())
    , settings(std::move(settings_))
{
    auto serialization = type->getSerialization(settings.serialization_info);
    auto enumerate_settings = settings.enumerate;
    serialization->enumerateStreams(
        enumerate_settings,
        [&](const ISerialization::SubstreamPath & path)
        {
            if (!ISerialization::isEphemeralSubcolumn(path, path.size()))
                stream_layouts.emplace(ISerialization::getSubstreamsCacheKeyForStream(path), resolveStreamLayout(path));
        },
        ISerialization::SubstreamData(serialization).withType(type));
}

CompressedColumnSizeEstimator::StreamLayout CompressedColumnSizeEstimator::resolveStreamLayout(const ISerialization::SubstreamPath & path) const
{
    /// As `MergeTreeDataPartWriterOnDisk::getSubstreamCodec` and `MergeTreeDataPartWriterWide::addStreams`: the codec applies to the
    /// data substreams, a structural substream keeps only its generic codecs, and a compressed block holds whole values.
    const DataTypePtr & substream_type = path.back().data.type;
    const bool is_data_substream = ISerialization::isSpecialCompressionAllowed(path);

    const auto whole_values = [&](size_t size) { return substream_type ? roundCompressBlockSizeToWholeValues(size, *substream_type) : size; };

    StreamLayout layout;
    layout.codec = CompressionCodecFactory::instance().get(
        codec_description, is_data_substream ? substream_type.get() : nullptr, default_codec, /*only_generic=*/ !is_data_substream);
    layout.block_size = whole_values(settings.max_compress_block_size);
    if (settings.adaptive_buffer_for_dynamic_subcolumns && ISerialization::isDynamicSubcolumn(path, path.size()))
        layout.adaptive_initial_size = whole_values(settings.adaptive_buffer_initial_size);
    const auto substream = path.back().type;
    layout.cuts_blocks_itself = substream == ISerialization::Substream::ObjectSharedDataData
        || substream == ISerialization::Substream::ObjectTypedSharedDataPathsData;
    return layout;
}

size_t CompressedColumnSizeEstimator::granuleRows(const IColumn & column) const
{
    /// The writer counts the bytes of all columns of the block, so the granules of a table with wide rows are smaller.
    if (column.empty())
        return settings.index_granularity;
    return computeIndexGranularity(
        column.size(),
        column.byteSize(),
        settings.index_granularity_bytes,
        settings.index_granularity,
        /*blocks_are_granules=*/ false,
        /*can_use_adaptive_index_granularity=*/ settings.index_granularity_bytes != 0);
}

std::unique_ptr<CompressedColumnSizeEstimator::Writer> CompressedColumnSizeEstimator::startWriter(const IColumn & column) const
{
    auto serialization = type->getSerialization(*type->getSerializationInfo(column, settings.serialization_info));
    std::unique_ptr<Writer> writer(new Writer(type, serialization, settings));

    Writer * raw = writer.get();
    auto enumerate_settings = settings.enumerate;
    serialization->enumerateStreams(
        enumerate_settings,
        [&](const ISerialization::SubstreamPath & path)
        {
            if (ISerialization::isEphemeralSubcolumn(path, path.size()))
                return;

            const String key = ISerialization::getSubstreamsCacheKeyForStream(path);
            auto it = stream_layouts.find(key);
            const StreamLayout layout = it != stream_layouts.end() ? it->second : resolveStreamLayout(path);
            CompressionCodecPtr codec = layout.codec;
            if (codec->needsVectorDimensionUpfront())
            {
                /// The dimension is set on the codec: an instance of this writer, not the shared one.
                codec = resolveStreamLayout(path).codec;
                raw->vector_codecs.push_back(codec);
            }
            const bool grow = !(layout.cuts_blocks_itself && layout.adaptive_initial_size == 0);
            raw->streams.push_back(
                {key, std::make_unique<CompressedSizeCalculator>(codec, layout.block_size, layout.adaptive_initial_size, grow)});
        },
        ISerialization::SubstreamData(serialization).withType(type).withColumn(column.getPtr()));
    writer->settings.getter = [raw](const ISerialization::SubstreamPath & path) -> WriteBuffer * { return raw->calculatorFor(path); };
    writer->settings.stream_mark_getter = [raw](const ISerialization::SubstreamPath & path) { return raw->markFor(path); };
    writer->writePrefix(column);
    return writer;
}

}
