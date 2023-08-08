//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Executor;
class Task;

// 声明了纯虚函数的类是一个抽象类。所以，用户不能创建类的实例，只能创建它的派生类的实例。
class Event : public std::enable_shared_from_this<Event> {
public:
	Event(Executor &executor);
	virtual ~Event() = default;

public:
	// virtual + (=0) 是虚函数，子类必须继承
	virtual void Schedule() = 0;
	// virtual + ({}) 是虚函数，子类不是必须继承，可直接调用，相当于没有任何操作
	//! Called right after the event is finished
	virtual void FinishEvent() {
	}
	//! Called after the event is entirely finished
	virtual void FinalizeFinish() {
	}

	void FinishTask();
	void Finish();

	void AddDependency(Event &event);
	bool HasDependencies() const {
		return total_dependencies != 0;
	}
	const vector<Event *> &GetParentsVerification() const;

	void CompleteDependency();

	void SetTasks(vector<shared_ptr<Task>> tasks);

	void InsertEvent(shared_ptr<Event> replacement_event);

	bool IsFinished() const {
		return finished;
	}

	virtual void PrintPipeline() {
	}

protected:
	Executor &executor;
	//! The current threads working on the event
	atomic<idx_t> finished_tasks;
	//! The maximum amount of threads that can work on the event
	atomic<idx_t> total_tasks;

	//! The amount of completed dependencies
	//! The event can only be started after the dependencies have finished executing
	atomic<idx_t> finished_dependencies;
	//! The total amount of dependencies
	idx_t total_dependencies;

	//! The events that depend on this event to run
	vector<weak_ptr<Event>> parents;
	//! Raw pointers to the parents (used for verification only)
	vector<Event *> parents_raw;

	//! Whether or not the event is finished executing
	atomic<bool> finished;
};

} // namespace duckdb
