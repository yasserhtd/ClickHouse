#include <algorithm>
#include <memory>
#include <optional>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CompressedColumnSizeEstimator.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Poco/String.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int BAD_QUERY_PARAMETER;
extern const int UNKNOWN_QUERY_PARAMETER;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Whether the bytes of a column of `type` do not depend on how its rows are split into writes, because every stream holds one
/// value after another. Others write something with every write: `LowCardinality` a header (with a numeric dictionary too,
/// although its values are represented by numbers), `Dynamic` and `JSON` their structure, `Map` its buckets, a sparse column
/// the end of its rows. The rows of those that do not complete a granule are buffered until it is complete; the others are
/// written as they come and only counted towards the granule.
bool isSplitIndependent(const IDataType & type)
{
    if (type.lowCardinality())
        return false;
    if (type.isValueRepresentedByNumber() || isStringOrFixedString(type))
        return true;
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
        return isSplitIndependent(*nullable->getNestedType());
    if (const auto * array = typeid_cast<const DataTypeArray *>(&type))
        return isSplitIndependent(*array->getNestedType());
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
        return std::ranges::all_of(tuple->getElements(), [](const DataTypePtr & element) { return isSplitIndependent(*element); });
    return false;
}

/// One run of a state, as MergeTree writes a column of one part. A sparse column after a dense one, or vice versa, starts a new
/// run.
struct Run
{
    ISerialization::KindStack kind_stack;
    std::unique_ptr<CompressedColumnSizeEstimator::Writer> writer;

    bool finished = false;
    /// The compressed bytes of this run and of the finished runs before it since the state was last read.
    UInt64 compressed_bytes = 0;
};

struct AggregateFunctionEstimateCompressedSizeData
{
    UInt64 merged_compressed_size = 0;

    /// Pending work: rows that do not fill a granule yet and the current run. Completed whenever the state is read, also through
    /// a const pointer (`serialize`, `merge`), which does not change what the state stands for.
    mutable MutableColumnPtr buffer;
    /// Whether `buffer` is a sparse column: the kind of the rows it takes (see `startBufferOfKind`).
    bool buffer_is_sparse = false;
    /// The rows the buffered granule holds when complete, as the block that started it sizes a granule.
    mutable size_t buffer_granule_rows = 0;
    mutable std::unique_ptr<Run> run;
};

/// A conversion of the rows, prepared once. The prepared function refers to the function it is prepared from without owning
/// it, so both are kept.
struct Conversion
{
    FunctionBasePtr function;
    ExecutableFunctionPtr prepared;
};

/// The parameters of the function, and the settings of the server and of the query the writer would use.
struct EstimateCompressedSizeConfig
{
    std::optional<String> codec;
    std::optional<UInt64> block_size_bytes;
    DataTypePtr target_type;
    CompressedColumnSizeEstimator::WriterSettings writer_settings;
};

/** Estimates the compressed size of a column stored as a given type: the rows are converted to the `target_type` parameter
  * (else kept in the type they arrive as), written as MergeTree writes a column of a wide part, and the compressed bytes are
  * read off the writer.
  */
class AggregateFunctionEstimateCompressedSize final
    : public IAggregateFunctionDataHelper<AggregateFunctionEstimateCompressedSizeData, AggregateFunctionEstimateCompressedSize>
{
private:
    const EstimateCompressedSizeConfig config;
    /// The type the rows arrive as.
    const DataTypePtr argument_type;
    /// The type the column is measured as: the `target_type` parameter when given, else the type of the argument.
    const DataTypePtr estimated_type;
    /// The conversion of the rows from `argument_type` to `estimated_type`, where they differ.
    const std::optional<Conversion> conversion;
    /// Whether the rows that do not complete a granule wait in the buffer for the next block, so that the granule is written at
    /// once (see `isSplitIndependent`).
    const bool buffer_partial_granules;
    /// The codec the server gives a large merged part (see `CompressedColumnSizeEstimator::WriterSettings::default_codec`).
    const CompressionCodecPtr default_codec;
    /// The `codec` parameter, or the default codec, as the `CODEC` clause of a column.
    const ASTPtr codec_description;
    /// Writes the rows as the MergeTree writer would; building it validates the codec against the estimated type.
    const CompressedColumnSizeEstimator estimator;
    /// Whether a sparse column is written as such with the server's serialization versions (`Nullable` only with
    /// `nullable_serialization_version = 'allow_sparse'`). Otherwise a table stores it dense, and so is it measured.
    const bool sparse_allowed;

    static ASTPtr parseCodecDescription(const String & codec)
    {
        ParserCodec codec_parser;
        auto ast = parseQuery(codec_parser, "(" + codec + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        /// The serialized values are compressed directly, so a declarative codec would measure nothing.
        CompressionCodecFactory::instance().checkCodecChainIsNotDeclarative(ast);
        return ast;
    }

    /// The cast to `to`; `prepare` builds the converters and so rejects a conversion that does not exist.
    static Conversion createCast(const DataTypePtr & from, const DataTypePtr & to)
    {
        ColumnsWithTypeAndName arguments{{from->createColumn(), from, ""}, {nullptr, std::make_shared<DataTypeString>(), ""}};
        Conversion conversion;
        conversion.function = createInternalCast(arguments[0], to, CastType::accurate, {}, nullptr);
        conversion.prepared = conversion.function->prepare(arguments);
        return conversion;
    }

    /// The settings of the writer, with `block_size_bytes` as both the smallest and the largest size of a compressed block.
    static CompressedColumnSizeEstimator::WriterSettings writerSettings(const EstimateCompressedSizeConfig & config)
    {
        auto settings = config.writer_settings;
        if (config.block_size_bytes)
        {
            settings.min_compress_block_size = *config.block_size_bytes;
            settings.max_compress_block_size = *config.block_size_bytes;
            settings.bulk.min_compress_block_size = *config.block_size_bytes;
        }
        return settings;
    }

    /// Ends the run as the writer ends a part. Idempotent, also after an exception: the writer is finished once.
    static void finishRun(Run & run)
    {
        if (run.finished)
            return;

        run.finished = true;
        auto writer = std::move(run.writer);
        run.compressed_bytes += writer->finish();
    }

    /// The current run, or a new one when there is none, when the state was read, or when the representation changed.
    Run & runFor(const Data & data_ref, const IColumn & rows) const
    {
        auto kind_stack = ISerialization::getKindStack(rows);
        if (data_ref.run && !data_ref.run->finished && data_ref.run->kind_stack == kind_stack)
            return *data_ref.run;

        auto run = std::make_unique<Run>();
        run->kind_stack = std::move(kind_stack);
        run->writer = estimator.startWriter(rows);
        if (data_ref.run)
        {
            finishRun(*data_ref.run);
            run->compressed_bytes = data_ref.run->compressed_bytes;
        }
        data_ref.run = std::move(run);
        return *data_ref.run;
    }

    /// `rows` of `argument_type` converted to `estimated_type`. A sparse column stays sparse through a conversion that keeps its
    /// default value the default, as a table stores the converted column (see `IExecutableFunction::executeWithoutReplicatedColumns`).
    ColumnPtr convertRows(const ColumnPtr & rows) const
    {
        ColumnsWithTypeAndName arguments{
            {rows, argument_type, ""},
            {DataTypeString().createColumnConst(rows->size(), estimated_type->getName()), std::make_shared<DataTypeString>(), ""}};
        return conversion->prepared->execute(arguments, estimated_type, rows->size(), /*dry_run=*/ false);
    }

    /// Serializes the rows of [offset, offset + limit) into the granules of the run, which hold `granule_rows` rows each, as the
    /// writer serializes a block.
    void serializeRows(const Data & data_ref, const IColumn & rows, size_t offset, size_t limit, size_t granule_rows) const
    {
        /// No rows start no run: the kind of an empty converted column is not the kind of the rows to come.
        if (limit == 0)
            return;

        if (!conversion)
        {
            runFor(data_ref, rows).writer->write(rows, offset, limit, granule_rows);
            return;
        }

        const ColumnPtr input = (offset == 0 && limit == rows.size()) ? rows.getPtr() : rows.cut(offset, limit);
        const ColumnPtr converted = convertRows(input);
        runFor(data_ref, *converted).writer->write(*converted, 0, limit, granule_rows);
    }

    /// The buffer of the state, of the kind of `rows`: a sparse column stays sparse.
    IColumn & bufferFor(Data & data_ref, const IColumn & rows) const
    {
        if (!data_ref.buffer)
        {
            data_ref.buffer = rows.cloneEmpty();
            data_ref.buffer_is_sparse = rows.isSparse();
        }
        return *data_ref.buffer;
    }

    /// Rows of another kind than the buffered ones start a new buffer, and a new run, as they start a new run when they are
    /// serialized at once: a group estimates a sparse part and a dense part of a column as the two parts they are.
    void startBufferOfKind(Data & data_ref, bool rows_are_sparse) const
    {
        if (data_ref.buffer && data_ref.buffer_is_sparse != rows_are_sparse)
        {
            flushBuffer(data_ref);
            data_ref.buffer.reset();
        }
    }

    /// Adds one row to the buffer of the state, which is flushed when it holds a granule, sized by the block the row comes from
    /// as the writer sizes the granules of a block. The per-row path of a `GROUP BY`: only the first row of a granule and the
    /// last one leave it.
    ALWAYS_INLINE void addRow(AggregateDataPtr __restrict place, const IColumn & column, size_t row_num) const
    {
        Data & data_ref = data(place);
        if (unlikely(!data_ref.buffer))
            bufferFor(data_ref, column);
        if (unlikely(data_ref.buffer_granule_rows == 0))
            data_ref.buffer_granule_rows = estimator.granuleRows(column);
        IColumn & buffer = *data_ref.buffer;
        buffer.insertFrom(column, row_num);

        if (unlikely(buffer.size() >= data_ref.buffer_granule_rows))
            flushBuffer(data_ref);
    }

    /// The rows of a sparse column that a table would store dense are added from its values: the buffer takes dense rows.
    ALWAYS_INLINE void addRowDense(AggregateDataPtr __restrict place, const IColumn & column, size_t row_num) const
    {
        const auto & sparse = assert_cast<const ColumnSparse &>(column);
        addRow(place, sparse.getValuesColumn(), sparse.getValueIndex(row_num));
    }

    /// Serializes rows after the rows added one at a time. The rows that do not complete a granule either wait in the buffer for
    /// the next block (`buffer_partial_granules`, and a sparse column, whose serialization marks the end of every write), so that
    /// the granule is written at once, or are written as they come and counted towards the granule, which the next block
    /// completes. Either way the result does not depend on how the query splits the rows into blocks.
    void addRows(Data & data_ref, const IColumn & rows, size_t offset, size_t limit) const
    {
        const size_t granule_rows = estimator.granuleRows(rows);
        if (!buffer_partial_granules && !rows.isSparse())
        {
            flushBuffer(data_ref);
            serializeRows(data_ref, rows, offset, limit, granule_rows);
            return;
        }

        startBufferOfKind(data_ref, rows.isSparse());
        if (data_ref.buffer && !data_ref.buffer->empty())
        {
            if (data_ref.buffer_granule_rows == 0)
                data_ref.buffer_granule_rows = granule_rows;
            const size_t taken
                = std::min(limit, data_ref.buffer_granule_rows - std::min(data_ref.buffer_granule_rows, data_ref.buffer->size()));
            data_ref.buffer->insertRangeFrom(rows, offset, taken);
            offset += taken;
            limit -= taken;
            if (data_ref.buffer->size() < data_ref.buffer_granule_rows)
                return;
            flushBuffer(data_ref);
        }

        const size_t whole_granules_rows = limit - limit % granule_rows;
        serializeRows(data_ref, rows, offset, whole_granules_rows, granule_rows);
        if (whole_granules_rows < limit)
        {
            bufferFor(data_ref, rows).insertRangeFrom(rows, offset + whole_granules_rows, limit - whole_granules_rows);
            data_ref.buffer_granule_rows = granule_rows;
        }
    }

    void flushBuffer(const Data & data_ref) const
    {
        if (!data_ref.buffer || data_ref.buffer->empty())
            return;

        /// The buffer holds one granule: complete, or the last one of the state.
        serializeRows(data_ref, *data_ref.buffer, 0, data_ref.buffer->size(), data_ref.buffer->size());
        data_ref.buffer = data_ref.buffer->cloneEmpty();
        data_ref.buffer_granule_rows = 0;
    }

    /// Completes the pending work and returns the compressed size of everything the state has seen.
    UInt64 totalSize(const Data & data_ref) const
    {
        flushBuffer(data_ref);

        UInt64 compressed_size = data_ref.merged_compressed_size;
        if (data_ref.run)
        {
            finishRun(*data_ref.run);
            compressed_size += data_ref.run->compressed_bytes;
        }
        return compressed_size;
    }

    /// Serializes the rows of [row_begin, row_end) that pass the condition of `-If` (`flags`) and are not NULL (`null_map`).
    void addRange(
        AggregateDataPtr __restrict place, const IColumn & column, size_t row_begin, size_t row_end, const UInt8 * flags, const UInt8 * null_map) const
    {
        if (row_begin >= row_end)
            return;

        const IColumn * rows = &column;
        size_t offset = row_begin;
        size_t limit = row_end - row_begin;
        ColumnPtr filtered;

        if (flags || null_map)
        {
            IColumn::Filter filter(limit);
            for (size_t i = row_begin; i < row_end; ++i)
                filter[i - row_begin] = (!flags || flags[i]) && (!null_map || !null_map[i]);

            const size_t passed = countBytesInFilter(filter);
            if (passed == 0)
                return;

            if (passed != limit)
            {
                filtered = (offset == 0 && limit == column.size())
                    ? column.filter(filter, passed)
                    : column.cut(offset, limit)->filter(filter, passed);
                rows = filtered.get();
                offset = 0;
                limit = passed;
            }
        }

        if (rows->isSparse() && !sparse_allowed)
        {
            ColumnPtr dense = recursiveRemoveSparse(rows->getPtr());
            filtered = std::move(dense);
            rows = filtered.get();
        }

        addRows(data(place), *rows, offset, limit);
    }

public:
    AggregateFunctionEstimateCompressedSize(const DataTypes & arguments, const Array & params, EstimateCompressedSizeConfig config_)
        : IAggregateFunctionDataHelper(arguments, params, std::make_shared<DataTypeUInt64>())
        , config(std::move(config_))
        , argument_type(arguments[0])
        , estimated_type(config.target_type ? config.target_type : argument_type)
        , conversion(estimated_type->equals(*argument_type) ? std::nullopt : std::optional(createCast(argument_type, estimated_type)))
        , buffer_partial_granules(!isSplitIndependent(*estimated_type))
        , default_codec(config.writer_settings.default_codec)
        , codec_description(config.codec ? parseCodecDescription(*config.codec) : default_codec->getFullCodecDescription())
        , estimator(estimated_type, codec_description, default_codec, writerSettings(config))
        , sparse_allowed(estimator.getSettings().serialization_info.canUseSparseSerialization(*estimated_type))
    {
    }

    String getName() const override { return "estimateCompressedSize"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const bool rows_are_sparse = columns[0]->isSparse() && sparse_allowed;
        startBufferOfKind(data(place), rows_are_sparse);
        if (rows_are_sparse || !columns[0]->isSparse())
            addRow(place, *columns[0], row_num);
        else
            addRowDense(place, *columns[0], row_num);
    }

    /// As the inherited version, with the kind of the rows looked up once for the block rather than once per row.
    void addBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const IColumn & column = *columns[0];
        const bool densify = column.isSparse() && !sparse_allowed;
        const bool rows_are_sparse = column.isSparse() && !densify;
        const UInt8 * flags = if_argument_pos >= 0 ? assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data() : nullptr;
        for (size_t i = row_begin; i < row_end; ++i)
        {
            if (!places[i] || (flags && !flags[i]))
                continue;
            AggregateDataPtr place = places[i] + place_offset;
            startBufferOfKind(data(place), rows_are_sparse);
            if (densify)
                addRowDense(place, column, i);
            else
                addRow(place, column, i);
        }
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos) const override
    {
        const UInt8 * flags = if_argument_pos >= 0 ? assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data() : nullptr;
        addRange(place, *columns[0], row_begin, row_end, flags, nullptr);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const UInt8 * flags = if_argument_pos >= 0 ? assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data() : nullptr;
        addRange(place, *columns[0], row_begin, row_end, flags, null_map);
    }

    /// A sparse column is serialized as sparse, as a part stores it.
    void addBatchSparseSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *) const override
    {
        addRange(place, *columns[0], row_begin, row_end, nullptr, nullptr);
    }

    /// With keys too: the sparse column itself reaches the buffer of each group, one row at a time, so that the rows of a group are
    /// measured sparse, as the part of the group would store them. The inherited version adds the values of the rows, dense.
    void addBatchSparse(
        size_t row_begin, size_t row_end, AggregateDataPtr * places, size_t place_offset, const IColumn ** columns, Arena * arena) const override
    {
        if (!sparse_allowed)
        {
            IAggregateFunctionDataHelper::addBatchSparse(row_begin, row_end, places, place_offset, columns, arena);
            return;
        }

        for (size_t i = row_begin; i < row_end; ++i)
        {
            if (!places[i])
                continue;
            AggregateDataPtr place = places[i] + place_offset;
            startBufferOfKind(data(place), /*rows_are_sparse=*/ true);
            addRow(place, *columns[0], i);
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merged_compressed_size += totalSize(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        writeVarUInt(totalSize(data(place)), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        readVarUInt(data(place).merged_compressed_size, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        Data & data_ref = data(place);
        const UInt64 compressed_size = totalSize(data_ref);

        /// The size is kept and the run is dropped: the next row of a growing window frame adds to it, while a repeated result
        /// for an unchanged frame does not count the run again.
        data_ref.merged_compressed_size = compressed_size;
        data_ref.run.reset();
        data_ref.buffer.reset();

        assert_cast<ColumnUInt64 &>(to).getData().push_back(compressed_size);
    }
};

/// The parameters, in any order: a number is the compressed block size, a string that names a data type is the target type, any
/// other string the codec. No codec is named like a data type.
EstimateCompressedSizeConfig parseParameters(const String & name, const Array & parameters)
{
    if (parameters.size() > 3)
        throw Exception(ErrorCodes::UNKNOWN_QUERY_PARAMETER, "Aggregate function {} accepts at most three parameters", name);

    const auto & type_factory = DataTypeFactory::instance();
    EstimateCompressedSizeConfig config;
    for (const auto & param : parameters)
    {
        if (param.getType() == Field::Types::String)
        {
            const String & value = param.safeGet<String>();

            if (DataTypePtr type = type_factory.tryGet(value))
            {
                if (config.target_type)
                    throw Exception(
                        ErrorCodes::BAD_QUERY_PARAMETER,
                        "Multiple data type parameters specified for {}. Expected at most one data type (target_type) parameter",
                        name);
                config.target_type = std::move(type);
                continue;
            }

            /// A string starting with the name of a type family is a mistyped type: report its error, not an unknown codec.
            const String family(value.begin(), std::find_if_not(value.begin(), value.end(), isWordCharASCII));
            if (type_factory.hasNameOrAlias(family) || type_factory.hasNameOrAlias(Poco::toLower(family)))
                type_factory.get(value);

            if (config.codec.has_value())
                throw Exception(
                    ErrorCodes::BAD_QUERY_PARAMETER,
                    "Multiple codec parameters specified for {}. Expected at most one string that is not a data type (codec)",
                    name);
            config.codec = value;
        }
        else if (param.getType() == Field::Types::UInt64)
        {
            if (config.block_size_bytes.has_value())
                throw Exception(
                    ErrorCodes::BAD_QUERY_PARAMETER,
                    "Multiple numeric type parameters specified for {}. Expected at most one numeric type (block_size_bytes) parameter",
                    name);

            UInt64 block_size_bytes = param.safeGet<UInt64>();
            if (block_size_bytes == 0)
                throw Exception(ErrorCodes::BAD_QUERY_PARAMETER, "block_size_bytes should be greater than 0");

            /// The limit of the writer too; it prevents absurd memory allocations from fuzzed queries.
            static constexpr UInt64 max_block_size_bytes = CompressedColumnSizeEstimator::WriterSettings::MAX_COMPRESS_BLOCK_SIZE;
            if (block_size_bytes > max_block_size_bytes)
                throw Exception(
                    ErrorCodes::BAD_QUERY_PARAMETER,
                    "block_size_bytes ({}) is too large, maximum is {}",
                    block_size_bytes, max_block_size_bytes);

            config.block_size_bytes = block_size_bytes;
        }
        else
        {
            throw Exception(
                ErrorCodes::UNKNOWN_QUERY_PARAMETER,
                "Invalid parameter type for {}. Expected String (codec or target_type) and/or UInt64 (block_size_bytes)",
                name);
        }
    }
    return config;
}

AggregateFunctionPtr createAggregateFunctionEstimateCompressedSize(
    const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings * settings)
{
    if (arguments.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one argument", name);

    auto config = parseParameters(name, parameters);
    /// Without a query (a type being parsed) the defaults of the query settings apply.
    config.writer_settings = CompressedColumnSizeEstimator::WriterSettings::fromServer(settings);

    return std::make_shared<AggregateFunctionEstimateCompressedSize>(arguments, parameters, std::move(config));
}

}

void registerAggregateFunctionEstimateCompressedSize(AggregateFunctionFactory & factory);
void registerAggregateFunctionEstimateCompressedSize(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Estimates the size, in bytes, that a column takes on disk when it is stored as a given type, without compressing it.

The rows are converted to the `target_type` parameter when it is given, as `accurateCast` does: a value that does not fit the target type is an error, also when the target type is `Nullable`. Without it, the column is estimated as the type the function receives it in. Like every aggregate function, it receives a `LowCardinality` column as its dictionary type, so `estimateCompressedSize(lc)` estimates `lc` as a plain column; to estimate the dictionary encoding, name it: `estimateCompressedSize('LowCardinality(String)')(lc)`.

The estimate follows the layout of a MergeTree wide part: every substream of the column (the dictionary and the indexes of a `LowCardinality` column, the null map of a `Nullable` column, the sizes of an `Array` column, the lengths of a `String` column) is compressed on its own; the codec applies to the data substreams, while the structural substreams keep only its generic codecs, as `CODEC(...)` on a table column does. The rows are serialized one granule at a time, whatever the size of the blocks the query reads, and a compressed block ends at a granule boundary once it holds `min_compress_block_size` bytes, as in a part.

The result is the number of bytes the column would take on disk (`column_data_compressed_bytes` in `system.parts_columns` for an existing column), so the estimates of one column for different target types and codecs are directly comparable: the smaller, the better. A sparse column stays sparse through a conversion that keeps its default value the default (`UInt64` to `Int64`), and is measured dense when the target type cannot be stored sparse (`LowCardinality(String)`).

The function has no table, so what the writer takes from a table it takes from the server's MergeTree settings (the `merge_tree` section of the configuration): the serialization versions of `String`, `Nullable` and `Map`, the layouts of `Dynamic`, `JSON` and `Variant` as a merged part stores them, the map buckets, `index_granularity`, `index_granularity_bytes`, `min_compress_block_size`, `max_compress_block_size`, and the adaptive buffers of the substreams of `Dynamic` and `JSON`. Without the `codec` parameter, the codec is the one the server gives a large merged part: the `default_compression_codec` MergeTree setting when it is set, else the `compression` section of the configuration, else `ZSTD(3)`. From the query it takes `low_cardinality_max_dictionary_size`, `low_cardinality_use_single_dictionary_for_part`, and the compressed block sizes when the MergeTree settings leave them unset. The settings of a particular table are not known to the function, and the granule size is estimated from this column alone, while a table sizes its granules by all its columns.

The rows of each `GROUP BY` group and of each window frame are serialized as if they were a part of their own, so the result also depends on how the rows are split between groups, threads and frames. Each state holds the rows of an incomplete granule and one compression buffer per substream, so a `GROUP BY` with many groups needs more memory than with most aggregate functions. A column that a table stores sparse is estimated sparse, except through the `-If` combinator, which receives it dense.

The intermediate state records the size only: merging the states of several groups gives the sum of their sizes, as the parts they would be.

<Note>
Without the `codec` parameter, the result depends on the default compression codec of the server.
See [Column Compression Codecs](/reference/statements/create/table#column_compression_codec).
</Note>
    )";
    FunctionDocumentation::Syntax syntax = "estimateCompressedSize([codec][, block_size_bytes][, target_type])(column)";
    FunctionDocumentation::Arguments arguments = {
        {"column", "Column of any type.", {"Any"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"codec", "String containing a compression codec or multiple comma-separated codecs in a single string.", {"String"}},
        {"block_size_bytes", "Size of a compressed block in bytes. When given, a block ends when it reaches this size, as if both [`max_compress_block_size`](/reference/settings/merge-tree-settings/max#max_compress_block_size) and [`min_compress_block_size`](/reference/settings/merge-tree-settings/min#min_compress_block_size) were set to it. When omitted, the server's settings apply (1048576 and 65536 bytes by default): a block ends at a granule boundary once it holds `min_compress_block_size` bytes, and when it reaches `max_compress_block_size`. Maximum allowed value is 256 MiB (268435456 bytes).", {"UInt64"}},
        {"target_type", "Name of a data type. The column is estimated as if it were stored as this type: its rows are converted to it, as `accurateCast` does, and compressed with the codec. When omitted, the column is estimated as the type the function receives it in. The parameters can be given in any order: a string that names a data type is the target type, any other string is the codec.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the estimated compressed size of the column in bytes.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Comparing a column with another representation of its data",
        R"(
CREATE TABLE compression_estimate_strings
(
    `s` String CODEC(LZ4)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO compression_estimate_strings
SELECT concat('category_', toString(number % 50)) FROM system.numbers LIMIT 100_000;

-- `LowCardinality(String)` would store this column in about 40 times fewer bytes than `String`.
SELECT
    estimateCompressedSize('LZ4')(s) AS as_string,
    estimateCompressedSize('LZ4', 'LowCardinality(String)')(s) AS as_low_cardinality
FROM compression_estimate_strings
        )",
        R"(
┌─as_string─┬─as_low_cardinality─┐
│     38347 │                947 │
└───────────┴────────────────────┘
        )"
    },
    {
        "Comparing a wider type for the same values under different codecs",
        R"(
-- `IPv6` stores the same addresses in 16 bytes instead of 4: with `LZ4` the 12 constant bytes of every address cost space, with
-- `ZSTD` they almost compress away.
SELECT
    estimateCompressedSize('LZ4')(ip) AS lz4_as_ipv4,
    estimateCompressedSize('LZ4', 'IPv6')(ip) AS lz4_as_ipv6,
    estimateCompressedSize('ZSTD')(ip) AS zstd_as_ipv4,
    estimateCompressedSize('ZSTD', 'IPv6')(ip) AS zstd_as_ipv6
FROM (SELECT toIPv4(number * 7919) AS ip FROM numbers(100000))
        )",
        R"(
┌─lz4_as_ipv4─┬─lz4_as_ipv6─┬─zstd_as_ipv4─┬─zstd_as_ipv6─┐
│      401748 │      531754 │       375725 │       379792 │
└─────────────┴─────────────┴──────────────┴──────────────┘
        )"
    },
    {
        "The estimate of an existing column against its size on disk",
        R"(
-- The estimate takes the codec of the column; without the codec parameter it would take the codec the server gives a large
-- merged part, which a small part may not have been written with.
SELECT
    (SELECT estimateCompressedSize('LZ4')(s) FROM compression_estimate_strings) AS estimate,
    column_data_compressed_bytes AS on_disk
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'compression_estimate_strings' AND column = 's' AND active
        )",
        R"(
┌─estimate─┬─on_disk─┐
│    38347 │   38347 │
└──────────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};
    factory.registerFunction(
        "estimateCompressedSize",
        {createAggregateFunctionEstimateCompressedSize, documentation, {.is_order_dependent = true, .is_window_function = true}});
}
}
