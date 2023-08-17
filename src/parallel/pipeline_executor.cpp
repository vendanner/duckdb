#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/limits.hpp"

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
#include <thread>
#include <chrono>
#endif

namespace duckdb {

PipelineExecutor::PipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p)
    : pipeline(pipeline_p), thread(context_p), context(context_p, thread, &pipeline_p) {
	D_ASSERT(pipeline.source_state);
	if (pipeline.sink) {
		local_sink_state = pipeline.sink->GetLocalSinkState(context);
		requires_batch_index = pipeline.sink->RequiresBatchIndex() && pipeline.source->SupportsBatchIndex();
		if (requires_batch_index) {
			auto &partition_info = local_sink_state->partition_info;
			if (!partition_info.batch_index.IsValid()) {
				// batch index is not set yet - initialize before fetching anything
				partition_info.batch_index = pipeline.RegisterNewBatchIndex();
				partition_info.min_batch_index = partition_info.batch_index;
			}
		}
	}
	local_source_state = pipeline.source->GetLocalSourceState(context, *pipeline.source_state);

	intermediate_chunks.reserve(pipeline.operators.size());
	intermediate_states.reserve(pipeline.operators.size());
	for (idx_t i = 0; i < pipeline.operators.size(); i++) {
		auto &prev_operator = i == 0 ? *pipeline.source : pipeline.operators[i - 1].get();
		auto &current_operator = pipeline.operators[i].get();

		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::Get(context.client), prev_operator.GetTypes());
		intermediate_chunks.push_back(std::move(chunk));

		auto op_state = current_operator.GetOperatorState(context);
		intermediate_states.push_back(std::move(op_state));

		if (current_operator.IsSink() && current_operator.sink_state->state == SinkFinalizeType::NO_OUTPUT_POSSIBLE) {
			// one of the operators has already figured out no output is possible
			// we can skip executing the pipeline
			FinishProcessing();
		}
	}
	InitializeChunk(final_chunk);
}

bool PipelineExecutor::TryFlushCachingOperators() {
	if (!started_flushing) {
		// Remainder of this method assumes any in process operators are from flushing
		D_ASSERT(in_process_operators.empty());
		started_flushing = true;
		flushing_idx = IsFinished() ? idx_t(finished_processing_idx) : 0;
	}

	// Go over each operator and keep flushing them using `FinalExecute` until empty
	while (flushing_idx < pipeline.operators.size()) {
		if (!pipeline.operators[flushing_idx].get().RequiresFinalExecute()) {
			flushing_idx++;
			continue;
		}

		// This slightly awkward way of increasing the flushing idx is to make the code re-entrant: We need to call this
		// method again in the case of a Sink returning BLOCKED.
		if (!should_flush_current_idx && in_process_operators.empty()) {
			should_flush_current_idx = true;
			flushing_idx++;
			continue;
		}

		auto &curr_chunk =
		    flushing_idx + 1 >= intermediate_chunks.size() ? final_chunk : *intermediate_chunks[flushing_idx + 1];
		auto &current_operator = pipeline.operators[flushing_idx].get();

		OperatorFinalizeResultType finalize_result;
		OperatorResultType push_result;

		if (in_process_operators.empty()) {
			StartOperator(current_operator);
			finalize_result = current_operator.FinalExecute(context, curr_chunk, *current_operator.op_state,
			                                                *intermediate_states[flushing_idx]);
			EndOperator(current_operator, &curr_chunk);
		} else {
			// Reset flag and reflush the last chunk we were flushing.
			finalize_result = OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		}

		push_result = ExecutePushInternal(curr_chunk, flushing_idx + 1);

		if (finalize_result == OperatorFinalizeResultType::HAVE_MORE_OUTPUT) {
			should_flush_current_idx = true;
		} else {
			should_flush_current_idx = false;
		}

		if (push_result == OperatorResultType::BLOCKED) {
			remaining_sink_chunk = true;
			return false;
		} else if (push_result == OperatorResultType::FINISHED) {
			break;
		}
	}
	return true;
}

/**
 * Pipeline 处理顺序是 Source->Operators->Sink
 *	Execute：从source 获取数据 inputdata，给ExecutePushInternal 消费
 *		收到ExecutePushInternal 返回 NEED_MORE_INPUT：
 *			若source 数据都已获取且已输出：退出，当前Pipeline 执行完毕
 *			若source 数据都已获取但存在in_process_operators：当前inputdata 给ExecutePushInternal
 *			若source 还有数据，getdata，给ExecutePushInternal
 *		收到ExecutePushInternal 返回 FINISHED：退出，当前Pipeline 执行结束
 *		ExecutePushInternal：将inputdata 给operators 消费，结果继续给Sink 消费
 *			收到Execute 返回 NEED_MORE_INPUT：等sink 消费后，向上层返回NEED_MORE_INPUT，需要继续输入新的 inputdata
 *			收到Execute 返回 FINISHED：返回FINISHED 表示直接结束
 *			收到Execute 返回 HAVE_MORE_OUTPUT：表示Execute 要再消费一次当前 inputdata，继续Execute(inputdata)
 *			Execute(DataChunk）：将inputdata 按顺序给operator 处理
 *				返回 NEED_MORE_INPUT 表示当前inputdata 所有operator 都已处理完毕，请求继续输入新的 inputdata
 *				返回 FINISHED：operator 不再处理，提前结束
 *				返回 HAVE_MORE_OUTPUT：当前inputdata 再输入，需要重新处理产生新的输出
 *
 * 初始状态 exhausted_source = false 和 in_process_operators.isempty
 * 	source 有数据，FetchFromSource 获取数据
 * 		-> pipeline.source->GetData
 * 		若source 返回FINISHED 表示source 数据已全部获取，设置 exhausted_source=true
 * 		否则调用ExecutePushInternal 执行Operators
 * 	ExecutePushInternal：operators->sink 执行
 * 		返回三种状态
 * 			NEED_MORE_INPUT： 上层循环继续，看情况输入不同的data 调此函数
 * 			FINISHED：上层结束循环，该pipeline 收尾
 * 			BLOCKED：上层直接退出循环，进入异常处理
 * 		Execute: operators 执行
 * 			返回三种状态
 * 				NEED_MORE_INPUT：上层返回 NEED_MORE_INPUT(表示当前批次数据已处理完)
 * 				FINISHED：上层返回 FINISHED
 * 				HAVE_MORE_OUTPUT：上层继续循环调用此函数，让operator 输出
 *
 * 当 exhausted_source=true，done_flushing=false 时
 * 	什么时候会发生这种情况？ FetchFromSource 一次调用将全部数据获取完毕
 * 	调用 TryFlushCachingOperators，内部也会调 ExecutePushInternal，执行一遍所有的 Operators
 *  一次数据处理流程：source->operatos->sink，反复调用直到 source 无数据；
 *  当一次性就能获取source 时，执行operatos->sink 即可无需再重复
 *
 * 当 in_process_operators 不为空时，表示有 operator 需要之前的数据重新再调用(HAVE_MORE_OUTPUT)
 * 	ExecutePushInternal(source_chunk)
 *
 * 二：ExecutePushInternal
 * 		operators->execute => sink
 * 		PipelineExecutor::ExecutePushInternal() 可以看做是 Pipeline 内的数据消费者。
 * 三：PushFinalize
 * 		sink->combine
 * @param max_chunks
 * @return
 */
PipelineExecuteResult PipelineExecutor::Execute(idx_t max_chunks) {
	D_ASSERT(pipeline.sink);
	auto &source_chunk = pipeline.operators.empty() ? final_chunk : *intermediate_chunks[0];
	for (idx_t i = 0; i < max_chunks; i++) {
		if (context.client.interrupted) {
			throw InterruptException();
		}

		OperatorResultType result;
		if (exhausted_source && done_flushing && !remaining_sink_chunk && in_process_operators.empty()) {
			break;
		} else if (remaining_sink_chunk) {
			// The pipeline was interrupted by the Sink. We should retry sinking the final chunk.
			result = ExecutePushInternal(final_chunk);
			remaining_sink_chunk = false;
		} else if (!in_process_operators.empty() && !started_flushing) {
			// The pipeline was interrupted by the Sink when pushing a source chunk through the pipeline. We need to
			// re-push the same source chunk through the pipeline because there are in_process operators, meaning that
			// the result for the pipeline
			D_ASSERT(source_chunk.size() > 0);
			result = ExecutePushInternal(source_chunk);
		} else if (exhausted_source && !done_flushing) {
			// The source was exhausted, try flushing all operators
			auto flush_completed = TryFlushCachingOperators();
			if (flush_completed) {
				done_flushing = true;
				break;
			} else {
				return PipelineExecuteResult::INTERRUPTED;
			}
		} else if (!exhausted_source) {
			// "Regular" path: fetch a chunk from the source and push it through the pipeline
			source_chunk.Reset();
			SourceResultType source_result = FetchFromSource(source_chunk);

			if (source_result == SourceResultType::BLOCKED) {
				return PipelineExecuteResult::INTERRUPTED;		// 处理方式和下面的sink 相同
			}

			if (source_result == SourceResultType::FINISHED) {
				exhausted_source = true;
				if (source_chunk.size() == 0) {
					continue;
				}
			}
			result = ExecutePushInternal(source_chunk);
		} else {
			throw InternalException("Unexpected state reached in pipeline executor");
		}

		// SINK INTERRUPT
		if (result == OperatorResultType::BLOCKED) {
			remaining_sink_chunk = true;
			// 返回 INTERRUPTED，上层PipelineTask 会TASK_BLOCKED，
			// 继而TaskScheduler 取消此任务调度
			// 只会发生在source/sink 异步的情况，这是个新特性，当前版本还在测试
			return PipelineExecuteResult::INTERRUPTED;
		}

		if (result == OperatorResultType::FINISHED) {
			break;
		}
	}

	if ((!exhausted_source || !done_flushing) && !IsFinished()) {
		// 大部分情况下，不应该执行到这里，只能是Finish 返回
		// NOT_FINISHED 返回是异常情况 PipelineTask 会抛异常
		return PipelineExecuteResult::NOT_FINISHED;
	}

	PushFinalize();

	return PipelineExecuteResult::FINISHED;
}

PipelineExecuteResult PipelineExecutor::Execute() {
	return Execute(NumericLimits<idx_t>::Maximum()); // uint64_t 最大值
}

OperatorResultType PipelineExecutor::ExecutePush(DataChunk &input) { // LCOV_EXCL_START
	return ExecutePushInternal(input);
} // LCOV_EXCL_STOP

void PipelineExecutor::FinishProcessing(int32_t operator_idx) {
	finished_processing_idx = operator_idx < 0 ? NumericLimits<int32_t>::Maximum() : operator_idx;
	in_process_operators = stack<idx_t>();
}

bool PipelineExecutor::IsFinished() {
	return finished_processing_idx >= 0;
}

/**
 * 执行 operators-> sink 流程
 *	Execute 执行operators.execute，返回状态
 *		NEED_MORE_INPUT：表示当前批次数据已处理完毕，sink 后返回上层，等待新批次数据调用
 *		FINISHED：operator结束，直接返回FINISHED，整个pipeline 任务结束
 *		HAVE_MORE_OUTPUT：用当前input 继续调用Execute
 *	只要不是FINISHED，operator 返回的data 会让Sink->sink 处理
 *	sink 函数只要不返回 FINISHED，继续循环
 *
 * @param input
 * @param initial_idx 默认是0
 * @return
 */
OperatorResultType PipelineExecutor::ExecutePushInternal(DataChunk &input, idx_t initial_idx) {
	D_ASSERT(pipeline.sink);
	if (input.size() == 0) { // LCOV_EXCL_START
		return OperatorResultType::NEED_MORE_INPUT;
	} // LCOV_EXCL_STOP

	// this loop will continuously push the input chunk through the pipeline as long as:
	// - the OperatorResultType for the Execute is HAVE_MORE_OUTPUT
	// - the Sink doesn't block
	while (true) {
		OperatorResultType result;
		// Note: if input is the final_chunk, we don't do any executing, the chunk just needs to be sinked
		if (&input != &final_chunk) {
			final_chunk.Reset();
			// 执行 operators.execute
			result = Execute(input, final_chunk, initial_idx);
			if (result == OperatorResultType::FINISHED) {
				return OperatorResultType::FINISHED;
			}
		} else {
			result = OperatorResultType::NEED_MORE_INPUT;
		}
		auto &sink_chunk = final_chunk;
		if (sink_chunk.size() > 0) {		// 说明operator 都已执行，轮到sink->sink 执行了
			StartOperator(*pipeline.sink);
			D_ASSERT(pipeline.sink);
			D_ASSERT(pipeline.sink->sink_state);
			OperatorSinkInput sink_input {*pipeline.sink->sink_state, *local_sink_state, interrupt_state};
			// operator.Sink
			auto sink_result = Sink(sink_chunk, sink_input);

			EndOperator(*pipeline.sink, nullptr);

			if (sink_result == SinkResultType::BLOCKED) {
				return OperatorResultType::BLOCKED;
			} else if (sink_result == SinkResultType::FINISHED) {
				// 直接结束整个Pipeline 任务
				FinishProcessing();
				return OperatorResultType::FINISHED;
			}
		}
		if (result == OperatorResultType::NEED_MORE_INPUT) {
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}
}

void PipelineExecutor::PushFinalize() {
	if (finalized) {
		throw InternalException("Calling PushFinalize on a pipeline that has been finalized already");
	}

	D_ASSERT(local_sink_state);

	finalized = true;

	// run the combine for the sink
	pipeline.sink->Combine(context, *pipeline.sink->sink_state, *local_sink_state);

	// flush all query profiler info
	for (idx_t i = 0; i < intermediate_states.size(); i++) {
		intermediate_states[i]->Finalize(pipeline.operators[i].get(), context);
	}
	pipeline.executor.Flush(thread);
	local_sink_state.reset();
}

// TODO: Refactoring the StreamingQueryResult to use Push-based execution should eliminate the need for this code
void PipelineExecutor::ExecutePull(DataChunk &result) {
	if (IsFinished()) {
		return;
	}
	auto &executor = pipeline.executor;
	try {
		D_ASSERT(!pipeline.sink);
		auto &source_chunk = pipeline.operators.empty() ? result : *intermediate_chunks[0];
		while (result.size() == 0 && !exhausted_source) {
			if (in_process_operators.empty()) {
				source_chunk.Reset();

				auto done_signal = make_shared<InterruptDoneSignalState>();
				interrupt_state = InterruptState(done_signal);
				SourceResultType source_result;

				// Repeatedly try to fetch from the source until it doesn't block. Note that it may block multiple times
				while (true) {
					source_result = FetchFromSource(source_chunk);

					// No interrupt happened, all good.
					if (source_result != SourceResultType::BLOCKED) {
						break;
					}

					// Busy wait for async callback from source operator
					done_signal->Await();
				}

				if (source_result == SourceResultType::FINISHED) {
					exhausted_source = true;
					if (source_chunk.size() == 0) {
						break;
					}
				}
			}
			if (!pipeline.operators.empty()) {
				auto state = Execute(source_chunk, result);
				if (state == OperatorResultType::FINISHED) {
					break;
				}
			}
		}
	} catch (const Exception &ex) { // LCOV_EXCL_START
		if (executor.HasError()) {
			executor.ThrowException();
		}
		throw;
	} catch (std::exception &ex) {
		if (executor.HasError()) {
			executor.ThrowException();
		}
		throw;
	} catch (...) {
		if (executor.HasError()) {
			executor.ThrowException();
		}
		throw;
	} // LCOV_EXCL_STOP
}

void PipelineExecutor::PullFinalize() {
	if (finalized) {
		throw InternalException("Calling PullFinalize on a pipeline that has been finalized already");
	}
	finalized = true;
	pipeline.executor.Flush(thread);
}

/**
 * 校准当前operators 要开始执行的operator 序号
 * 	一般情况时从 initial_idx 开始执行
 * 	当之前有operator 还没处理结束(HAVE_MORE_OUTPUT)，将序号设置为之前的operator 序号
 *
 * @param current_idx 后续要执行的序号
 * @param initial_idx 初始序号
 */
void PipelineExecutor::GoToSource(idx_t &current_idx, idx_t initial_idx) {
	// we go back to the first operator (the source)
	current_idx = initial_idx;
	if (!in_process_operators.empty()) {
		// ... UNLESS there is an in process operator
		// if there is an in-process operator, we start executing at the latest one
		// for example, if we have a join operator that has tuples left, we first need to emit those tuples
		current_idx = in_process_operators.top();
		in_process_operators.pop();
	}
	D_ASSERT(current_idx >= initial_idx);
}

/**
 * 按顺序执行operator
 *	GoToSource： 校准下面从哪个opeartor 开始执行
 *		存在 in_process_operators，直接从in_process_operators 开始，
 *		因为当前输入的数据，在in_process_operators 之前的operator 上次已执行没必要重新执行
 *	判断operators 是否都已执行，返回 NEED_MORE_INPUT
 *	while：遍历执行operator
 *		operator 返回HAVE_MORE_OUTPUT： in_process_operators，
 *			Execute(idx_t max_chunks) 函数中决定是否将之前数据重新调用 operators-sink 流程
 *		operator 返回无数据，重置下一个执行operator 为初始(重新获取数据)
 *		执行的operator 序号++
 * 	返回三种状态
 * 		NEED_MORE_INPUT：上层继续往上 NEED_MORE_INPUT(表示当前批次数据已处理完)
 * 		FINISHED：上层继续往上 FINISHED，意为结束此Pipeline
 * 		HAVE_MORE_OUTPUT：上层继续循环调用此函数(数据重复)，让operator 输出
 *
 * @param input
 * @param result
 * @param initial_idx 开始执行的operator 序号(在生成pipeline时，确定好了序号)
 * @return
 */
OperatorResultType PipelineExecutor::Execute(DataChunk &input, DataChunk &result, idx_t initial_idx) {
	if (input.size() == 0) { // LCOV_EXCL_START
		return OperatorResultType::NEED_MORE_INPUT;
	} // LCOV_EXCL_STOP
	D_ASSERT(!pipeline.operators.empty());

	idx_t current_idx;
	GoToSource(current_idx, initial_idx);
	if (current_idx == initial_idx) {
		current_idx++;
	}
	// 全部 operators 都已执行
	if (current_idx > pipeline.operators.size()) {
		result.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	while (true) {
		if (context.client.interrupted) {
			throw InterruptException();
		}
		// now figure out where to put the chunk
		// if current_idx is the last possible index (>= operators.size()) we write to the result
		// otherwise we write to an intermediate chunk

		// 每个operator 都属于自己的输出 DataChunk，
		// 最后一个operator 的输出 DataChunk 是result
		auto current_intermediate = current_idx;
		auto &current_chunk =
		    current_intermediate >= intermediate_chunks.size() ? result : *intermediate_chunks[current_intermediate];
		current_chunk.Reset();
		if (current_idx == initial_idx) {
			// we went back to the source: we need more input
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			auto &prev_chunk =
			    current_intermediate == initial_idx + 1 ? input : *intermediate_chunks[current_intermediate - 1];
			auto operator_idx = current_idx - 1;
			auto &current_operator = pipeline.operators[operator_idx].get();

			// if current_idx > source_idx, we pass the previous operators' output through the Execute of the current
			// operator
			StartOperator(current_operator);
			auto result = current_operator.Execute(context, prev_chunk, current_chunk, *current_operator.op_state,
			                                       *intermediate_states[current_intermediate - 1]);
			EndOperator(current_operator, &current_chunk);
			if (result == OperatorResultType::HAVE_MORE_OUTPUT) {
				// more data remains in this operator
				// push in-process marker
				// 保存哪些 operator 正在处理当前批次数据，上层不需要再获取新数据，
				// 将当前批次数据重新调用
				in_process_operators.push(current_idx);
			} else if (result == OperatorResultType::FINISHED) {
				D_ASSERT(current_chunk.size() == 0);
				// 直接结束整个Pipeline 任务
				FinishProcessing(current_idx);
				return OperatorResultType::FINISHED;
			}
			current_chunk.Verify();
		}

		if (current_chunk.size() == 0) {
			// no output from this operator!
			// 无数据返回，若是执行的第一个operator，直接退出
			// 若不是执行的第一个operator，重置下一个执行operator 为初始(重新获取数据)
			//		类似于 filter 操作将数据全部过滤了，那么后续操作没必要执行
			if (current_idx == initial_idx) {
				// if we got no output from the scan, we are done
				break;
			} else {
				// if we got no output from an intermediate op
				// we go back and try to pull data from the source again
				GoToSource(current_idx, initial_idx);
				continue;
			}
		} else {
			// we got output! continue to the next operator
			current_idx++;
			if (current_idx > pipeline.operators.size()) {
				// if we got output and are at the last operator, we are finished executing for this output chunk
				// return the data and push it into the chunk
				break;
			}
		}
	}
	return in_process_operators.empty() ? OperatorResultType::NEED_MORE_INPUT : OperatorResultType::HAVE_MORE_OUTPUT;
}

void PipelineExecutor::SetTaskForInterrupts(weak_ptr<Task> current_task) {
	interrupt_state = InterruptState(std::move(current_task));
}

SourceResultType PipelineExecutor::GetData(DataChunk &chunk, OperatorSourceInput &input) {
	//! Testing feature to enable async source on every operator
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (debug_blocked_source_count < debug_blocked_target_count) {
		debug_blocked_source_count++;

		auto &callback_state = input.interrupt_state;
		std::thread rewake_thread([callback_state] {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			callback_state.Callback();
		});
		rewake_thread.detach();

		return SourceResultType::BLOCKED;
	}
#endif

	return pipeline.source->GetData(context, chunk, input);
}

SinkResultType PipelineExecutor::Sink(DataChunk &chunk, OperatorSinkInput &input) {
	//! Testing feature to enable async sink on every operator
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	if (debug_blocked_sink_count < debug_blocked_target_count) {
		debug_blocked_sink_count++;

		auto &callback_state = input.interrupt_state;
		std::thread rewake_thread([callback_state] {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			callback_state.Callback();
		});
		rewake_thread.detach();

		return SinkResultType::BLOCKED;
	}
#endif
	return pipeline.sink->Sink(context, chunk, input);
}

/**
 * 从Source 获取数据
 * 	pipeline.source->GetData 获取一批数据
 * @param result
 * @return
 */
SourceResultType PipelineExecutor::FetchFromSource(DataChunk &result) {
	StartOperator(*pipeline.source);

	OperatorSourceInput source_input = {*pipeline.source_state, *local_source_state, interrupt_state};
	auto res = GetData(result, source_input);

	// Ensures Sinks only return empty results when Blocking or Finished
	D_ASSERT(res != SourceResultType::BLOCKED || result.size() == 0);

	if (requires_batch_index && res != SourceResultType::BLOCKED) {
		idx_t next_batch_index;
		if (result.size() == 0) {
			next_batch_index = NumericLimits<int64_t>::Maximum();
		} else {
			next_batch_index =
			    pipeline.source->GetBatchIndex(context, result, *pipeline.source_state, *local_source_state);
			next_batch_index += pipeline.base_batch_index;
		}
		auto &partition_info = local_sink_state->partition_info;
		if (next_batch_index != partition_info.batch_index.GetIndex()) {
			// batch index has changed - update it
			if (partition_info.batch_index.GetIndex() > next_batch_index) {
				throw InternalException(
				    "Pipeline batch index - gotten lower batch index %llu (down from previous batch index of %llu)",
				    next_batch_index, partition_info.batch_index.GetIndex());
			}
			auto current_batch = partition_info.batch_index.GetIndex();
			partition_info.batch_index = next_batch_index;
			// call NextBatch before updating min_batch_index to provide the opportunity to flush the previous batch
			pipeline.sink->NextBatch(context, *pipeline.sink->sink_state, *local_sink_state);
			partition_info.min_batch_index = pipeline.UpdateBatchIndex(current_batch, next_batch_index);
		}
	}

	EndOperator(*pipeline.source, &result);

	return res;
}

void PipelineExecutor::InitializeChunk(DataChunk &chunk) {
	auto &last_op = pipeline.operators.empty() ? *pipeline.source : pipeline.operators.back().get();
	chunk.Initialize(Allocator::DefaultAllocator(), last_op.GetTypes());
}

void PipelineExecutor::StartOperator(PhysicalOperator &op) {
	if (context.client.interrupted) {
		throw InterruptException();
	}
	context.thread.profiler.StartOperator(&op);
}

void PipelineExecutor::EndOperator(PhysicalOperator &op, optional_ptr<DataChunk> chunk) {
	context.thread.profiler.EndOperator(chunk);

	if (chunk) {
		chunk->Verify();
	}
}

} // namespace duckdb
