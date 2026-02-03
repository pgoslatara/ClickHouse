#include <IO/SharedThreadPools.h>
#include <Storages/ObjectStorage/Utils.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/logger_useful.h>
#include <Common/threadPoolCallbackRunner.h>
#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Storages/ObjectStorage/DataLakes/DeletionVectorTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteObject.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>

namespace DB::Setting
{
extern const SettingsNonZeroUInt64 max_block_size;
}
namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace DB::Iceberg
{

Poco::JSON::Array::Ptr IcebergPositionDeleteTransform::getSchemaFields()
{
    Poco::JSON::Array::Ptr pos_delete_schema = new Poco::JSON::Array;
    Poco::JSON::Object::Ptr field_pos = new Poco::JSON::Object;
    field_pos->set(Iceberg::f_id, IcebergPositionDeleteTransform::positions_column_field_id);
    field_pos->set(Iceberg::f_name, IcebergPositionDeleteTransform::positions_column_name);
    field_pos->set(Iceberg::f_required, true);
    field_pos->set(Iceberg::f_type, "long");

    Poco::JSON::Object::Ptr field_filename = new Poco::JSON::Object;
    field_filename->set(Iceberg::f_id, IcebergPositionDeleteTransform::data_file_path_column_field_id);
    field_pos->set(Iceberg::f_name, IcebergPositionDeleteTransform::data_file_path_column_name);
    field_pos->set(Iceberg::f_required, true);
    field_pos->set(Iceberg::f_type, "string");

    pos_delete_schema->add(field_filename);
    pos_delete_schema->add(field_pos);
    return pos_delete_schema;
}

void IcebergPositionDeleteTransform::initializeDeleteSources()
{
    /// Create filter on the data object to get interested rows
    auto iceberg_data_path = iceberg_object_info->info.data_object_file_path_key;
    ASTPtr where_ast = makeASTFunction(
        "equals",
        make_intrusive<ASTIdentifier>(IcebergPositionDeleteTransform::data_file_path_column_name),
        make_intrusive<ASTLiteral>(Field(iceberg_data_path)));

    for (const auto & position_deletes_object : iceberg_object_info->info.position_deletes_objects)
    {

        auto object_path = position_deletes_object.file_path;
        auto object_metadata = object_storage->getObjectMetadata(object_path, /*with_tags=*/ false);
        auto object_info = RelativePathWithMetadata{object_path, object_metadata};


        String format = position_deletes_object.file_format;
        if (boost::to_lower_copy(format) != "parquet")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Position deletes are supported only for parquet format");

        Block initial_header;
        {
            std::unique_ptr<ReadBuffer> read_buf_schema = createReadBuffer(object_info, object_storage, context, log);
            auto schema_reader = FormatFactory::instance().getSchemaReader(format, *read_buf_schema, context);
            auto columns_with_names = schema_reader->readSchema();
            ColumnsWithTypeAndName initial_header_data;
            for (const auto & elem : columns_with_names)
            {
                initial_header_data.push_back(ColumnWithTypeAndName(elem.type, elem.name));
            }
            initial_header = Block(initial_header_data);
        }

        CompressionMethod compression_method = chooseCompressionMethod(object_path, "auto");

        delete_read_buffers.push_back(createReadBuffer(object_info, object_storage, context, log));

        auto syntax_result = TreeRewriter(context).analyze(where_ast, initial_header.getNamesAndTypesList());
        ExpressionAnalyzer analyzer(where_ast, syntax_result, context);
        std::optional<ActionsDAG> actions = analyzer.getActionsDAG(true);
        std::shared_ptr<const ActionsDAG> actions_dag_ptr = [&actions]()
        {
            if (actions.has_value())
                return std::make_shared<const ActionsDAG>(std::move(actions.value()));
            return std::shared_ptr<const ActionsDAG>();
        }();

        auto delete_format = FormatFactory::instance().getInput(
            format,
            *delete_read_buffers.back(),
            initial_header,
            context,
            context->getSettingsRef()[DB::Setting::max_block_size],
            format_settings,
            parser_shared_resources,
            std::make_shared<FormatFilterInfo>(actions_dag_ptr, context, nullptr, nullptr, nullptr),
            true /* is_remote_fs */,
            compression_method);

        delete_sources.push_back(std::move(delete_format));
    }
}

size_t IcebergPositionDeleteTransform::getColumnIndex(const std::shared_ptr<IInputFormat> & delete_source, const String & column_name)
{
    const auto & delete_header = delete_source->getOutputs().back().getHeader();
    for (size_t i = 0; i < delete_header.getNames().size(); ++i)
    {
        if (delete_header.getNames()[i] == column_name)
        {
            return i;
        }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not find column {} in chunk", column_name);
}

void IcebergBitmapPositionDeleteTransform::transform(Chunk & chunk)
{
    DeletionVectorTransform::transform(chunk, bitmap);
}

void IcebergBitmapPositionDeleteTransform::initialize()
{
    if (delete_sources.empty())
        return;

    std::mutex bitmap_mutex;

    /// Use ClickHouse's thread pool infrastructure for proper task scheduling
    auto & thread_pool = DB::getFormatParsingThreadPool().get();
    DB::ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, DB::ThreadName::POLYGON_DICT_LOAD);

    for (auto & delete_source : delete_sources)
    {
        runner.enqueueAndKeepTrack(
            [&, delete_source_ptr = delete_source.get()]()
            {
                while (auto delete_chunk = delete_source_ptr->read())
                {
                    auto position_index = getColumnIndex(
                        std::shared_ptr<IInputFormat>(delete_source_ptr, [](IInputFormat *) { }),
                        IcebergPositionDeleteTransform::positions_column_name);
                    auto filename_index = getColumnIndex(
                        std::shared_ptr<IInputFormat>(delete_source_ptr, [](IInputFormat *) { }),
                        IcebergPositionDeleteTransform::data_file_path_column_name);

                    auto position_column = delete_chunk.getColumns()[position_index];
                    auto filename_column = delete_chunk.getColumns()[filename_index];

                    std::lock_guard<std::mutex> lock(bitmap_mutex);
                    for (size_t i = 0; i < delete_chunk.getNumRows(); ++i)
                    {
                        bitmap.add(position_column->get64(i));
                    }
                }
            });
    }

    /// Wait for all tasks to complete and rethrow any exceptions
    runner.waitForAllToFinishAndRethrowFirstError();
}

void IcebergStreamingPositionDeleteTransform::worker()
{
    {
        std::unique_lock signal_mutex_lock(signal_mutex);
        if (states[waiting_for_new_chunk_index] == State::NON_EMPTY || states[waiting_for_new_chunk_index] == State::FINISHED)
        {
            return;
        }
        states[waiting_for_new_chunk_index] = State::NON_EMPTY;
    }

    size_t delete_source_index;
    while (orders_queue.pop(delete_source_index))
    {
        auto & delete_source = delete_sources[delete_source_index].delete_source;
        auto chunk = delete_source->read();
        chunks_blocking_queue.push({delete_source_index, chunk});
    }
}


void IcebergStreamingPositionDeleteTransform::initialize()
{
    for (size_t i = 0; i < delete_sources.size(); ++i)
    {
        auto & delete_source = delete_sources[i];
        size_t position_index = getColumnIndex(delete_source, IcebergPositionDeleteTransform::positions_column_name);
        size_t filename_index = getColumnIndex(delete_source, IcebergPositionDeleteTransform::data_file_path_column_name);

        delete_source_column_indices.push_back(PositionDeleteFileIndexes{
            .filename_index = filename_index,
            .position_index = position_index
        });
        auto latest_chunk = delete_source->read();
        iterator_at_latest_chunks.push_back(0);
        if (latest_chunk.hasRows())
        {
            size_t first_position_value_in_delete_file = latest_chunk.getColumns()[delete_source_column_indices.back().position_index]->get64(0);
            latest_positions.insert(std::pair<size_t, size_t>{first_position_value_in_delete_file, i});
        }
        latest_chunks.push_back(std::move(latest_chunk));
    }
}


// struct PositionDeleteFileChunkWithPosition
// {
//     Chunk chunk;
//     size_t position = 0;

//     const size_t position_index;

//     PositionDeleteFileChunkWithPosition(Chunk chunk_, size_t position_index_)
//         : chunk(std::move(chunk_))
//         , position_index(position_index_)
//     {
//     }

//     std::optional<size_t> current() const
//     {
//         if (position >= chunk.getNumRows())
//             return std::nullopt;
//         return chunk.getColumns()[position_index]->get64(position);
//     }

//     std::optional<size_t> next()
//     {
//         if (position >= chunk.getNumRows())
//             return std::nullopt;
//         return chunk.getColumns()[position_index]->get64(position++);
//     }
// };

// class PositionDeleteFileIterator
// {
// public:
//     PositionDeleteFileIterator();

//     virtual void next() = 0;

//     virtual std::optional<PositionDeleteFileChunkWithPosition> current() const = 0;


//     virtual ~PositionDeleteFileIterator() = 0;
// };

void IcebergStreamingPositionDeleteTransform::pullNewChunkForSource(size_t delete_source_index)
{
    auto latest_chunk = delete_sources[delete_source_index]->read();
    if (latest_chunk.hasRows())
    {
        size_t first_position_value_in_delete_file
            = latest_chunk.getColumns()[delete_source_column_indices[delete_source_index].position_index]->get64(0);
        latest_positions.insert(std::pair<size_t, size_t>{first_position_value_in_delete_file, delete_source_index});
    }

    iterator_at_latest_chunks[delete_source_index] = 0;
    latest_chunks[delete_source_index] = std::move(latest_chunk);
}


// void IcebergStreamingPositionDeleteTransform::getNewChunkFromSource(size_t delete_source_index)
// {
//     // auto latest_chunk = delete_sources[delete_source_index]->read();
//     // if (latest_chunk.hasRows())
//     // {
//     //     size_t first_position_value_in_delete_file
//     //         = latest_chunk.getColumns()[delete_source_column_indices[delete_source_index].position_index]->get64(0);
//     //     latest_positions.insert(std::pair<size_t, size_t>{first_position_value_in_delete_file, delete_source_index});
//     // }
//     while (true)
//     {
//         auto [current_delete_source_index, chunk] = chunks_blocking_queue.pop();
//         std::unique_lock signal_mutex_lock(signal_mutex);
//         if (chunk.hasRows())
//         {
//             pulled_chunks_by_stream[current_delete_source_index].push_back(std::move(chunk));
//             if (pulled_chunks_by_stream[current_delete_source_index].size() < max_chunks_per_stream)
//             {
//                 orders_queue.push(current_delete_source_index);
//             }
//             if (current_delete_source_index == waiting_for_new_chunk_index)
//             {
//                 signal_cv.notify_all();
//             }
//         }
//         else
//         {
//             finished_streams[current_delete_source_index] = true;
//         }
//     }
// }

void IcebergStreamingPositionDeleteTransform::getNewChunkFromSource(size_t delete_source_index)
{
    std::unique_lock signal_mutex_lock(signal_mutex);
    waiting_for_new_chunk_index = delete_source_index;
    iterator_at_latest_chunks[delete_source_index] = 0;
    if (pulled_chunks_by_stream[delete_source_index].empty())
    {
        if (states[delete_source_index] == State::NON_EMPTY)
        {
            signal_cv.wait(
                signal_mutex_lock,
                [&] { return !pulled_chunks_by_stream[delete_source_index].empty() || states[delete_source_index] == State::FINISHED; });
            latest_chunks[delete_source_index] = std::move(pulled_chunks_by_stream[delete_source_index].front());
            if (pulled_chunks_by_stream[delete_source_index].size() == max_chunks_per_stream)
            {
                orders_queue.push(delete_source_index);
            }
        }
        if (states[delete_source_index] == State::EMPTY)
        {
            states[delete_source_index] = State::NON_EMPTY;
            signal_mutex_lock.unlock();
            latest_chunks[delete_source_index] = delete_sources[delete_source_index]->read();
            orders_queue.push(delete_source_index);
            signal_mutex_lock.lock();
            states[delete_source_index] = State::EMPTY;
            return;
        }
        if (finished_streams[delete_source_index])
        {
            latest_chunks[delete_source_index] = Chunk();
        }
    }
    else
    {
        latest_chunks[delete_source_index] = pulled_chunks_by_stream[delete_source_index].pop_front();
    }
}

void IcebergStreamingPositionDeleteTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    IColumn::Filter filter(num_rows, true);
    size_t num_rows_after_filtration = chunk.getNumRows();
    auto chunk_info = chunk.getChunkInfos().get<ChunkInfoRowNumbers>();
    if (!chunk_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ChunkInfoRowNumbers does not exist");

    size_t num_indices = chunk_info->applied_filter.has_value() ? chunk_info->applied_filter->size() : chunk.getNumRows();

    /// We get chunks in order of increasing row number because:
    ///  * this transform should be immediately after the IInputFormat
    ///    (typically ParquetV3BlockInputFormat) in the pipeline,
    ///  * IInputFormat outputs chunks in order of row number even if it uses multiple threads
    ///    internally; for parquet IcebergMetadata::modifyFormatSettings sets
    ///    `format_settings.parquet.preserve_order = true` to ensure this, other formats return
    ///    chunks in order by default.
    if (previous_chunk_end_offset && previous_chunk_end_offset.value() > chunk_info->row_num_offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunks offsets should increase.");
    previous_chunk_end_offset = chunk_info->row_num_offset + num_indices;

    size_t idx_in_chunk = 0;
    for (size_t i = 0; i < num_indices; i++)
    {
        if (!chunk_info->applied_filter.has_value() || chunk_info->applied_filter.value()[i])
        {
            size_t row_idx = chunk_info->row_num_offset + i;

            while (!latest_positions.empty())
            {
                auto it = latest_positions.begin();
                if (it->first < row_idx)
                {
                    size_t delete_source_index = it->second;
                    latest_positions.erase(it);
                    if (iterator_at_latest_chunks[delete_source_index] + 1 >= latest_chunks[delete_source_index].getNumRows()
                        && latest_chunks[delete_source_index].getNumRows() > 0)
                    {
                        fetchNewChunkFromSource(delete_source_index);
                    }
                    else
                    {
                        ++iterator_at_latest_chunks[delete_source_index];
                        auto position_index = delete_source_column_indices[delete_source_index].position_index;
                        size_t next_index_value_in_positional_delete_file
                            = latest_chunks[delete_source_index].getColumns()[position_index]->get64(
                                iterator_at_latest_chunks[delete_source_index]);
                        latest_positions.insert(std::pair<size_t, size_t>{next_index_value_in_positional_delete_file, delete_source_index});
                    }
                }
                else if (it->first == row_idx)
                {
                    filter[idx_in_chunk] = false;

                    if (chunk_info->applied_filter.has_value())
                        chunk_info->applied_filter.value()[i] = false;

                    --num_rows_after_filtration;
                    break;
                }
                else
                    break;
            }

            idx_in_chunk += 1;
        }
    }
    chassert(idx_in_chunk == chunk.getNumRows());

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->filter(filter, -1);

    if (!chunk_info->applied_filter.has_value())
        chunk_info->applied_filter.emplace(std::move(filter));

    chunk.setColumns(std::move(columns), num_rows_after_filtration);
}
}

#endif
