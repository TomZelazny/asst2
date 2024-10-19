#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

Job::Job(IRunnable* runnable, int num_total_tasks, int job_id, TaskID task_id): runnable(runnable), num_total_tasks(num_total_tasks), job_id(job_id), task_id(task_id) {}

Job::~Job() {}

Task::Task(IRunnable* runnable, int num_total_tasks, TaskID task_id, std::vector<TaskID> deps): runnable(runnable), num_total_tasks(num_total_tasks), task_id(task_id), deps(deps) {}

Task::~Task() {}

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

void TaskSystemParallelThreadPoolSleeping::worker_function() {
    while (true) {
        Job job(nullptr, 0, 0, 0);
        bool should_process_waiting = false;
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [this] { return !job_queue.empty() || stop; });
            if (stop && job_queue.empty() && waiting_tasks.empty()) {
                break;
            }
            if (!job_queue.empty()) {
                job = job_queue.front();
                job_queue.pop();
            } else {
                should_process_waiting = true;
            }
        }
        if (should_process_waiting) {
            process_waiting_tasks();
            continue;
        }
        if (job.runnable != nullptr) {
            job.runnable->runTask(job.job_id, job.num_total_tasks);
            if (task_remaining_jobs[job.task_id].fetch_sub(1) == 1) {
                remaining_tasks -= 1;
                cv.notify_all();
                process_waiting_tasks();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::process_waiting_tasks() {
    std::vector<Task> ready_tasks;
    {
        std::unique_lock<std::mutex> lock(mutex);
        for (auto it = waiting_tasks.begin(); it != waiting_tasks.end(); ) {
            bool ready = true;

            for (int i = 0; i < it->deps.size(); i++) {
                if (task_remaining_jobs[it->deps[i]] != 0) {
                    ready = false;
                    break;
                }
            }
            if (ready) {
                ready_tasks.push_back(*it);
                it = waiting_tasks.erase(it);
            } else {
                it++;
            }
        }

        for (int i = 0; i < ready_tasks.size(); i++) {
            for (int j = 0; j < ready_tasks[i].num_total_tasks; j++) {
                job_queue.push(Job(ready_tasks[i].runnable, ready_tasks[i].num_total_tasks, j, ready_tasks[i].task_id));
            }
        }
        if (!ready_tasks.empty()) {
            cv.notify_all();
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    stop = false;
    remaining_tasks = 0;
    for (int i = 0; i < num_threads; i++) {
        worker_thread_pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::worker_function, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    stop = true;
    cv.notify_all();
    for (int i = 0; i < worker_thread_pool.size(); i++) {
        worker_thread_pool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    task_remaining_jobs.emplace_back(num_total_tasks);
    remaining_tasks += 1;

    {
        std::lock_guard<std::mutex> lock(mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            job_queue.push(Job(runnable, num_total_tasks, i, 0));
        }
    }

    cv.notify_all();
    
    // Wait for all tasks to finish
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [this] { return task_remaining_jobs[0] == 0; });

    task_remaining_jobs.pop_back();
    remaining_tasks -= 1;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    TaskID task_id;
    remaining_tasks += 1;

    std::unique_lock<std::mutex> lock(mutex);
    task_id = task_remaining_jobs.size();
    task_remaining_jobs.emplace_back(num_total_tasks);

    bool ready = true;
    for (int i = 0; i < deps.size(); i++) {
        if (task_remaining_jobs[deps[i]] != 0) {
            ready = false;
            break;
        }
    }

    if (ready) {
        for (int i = 0; i < num_total_tasks; i++) {
            job_queue.push(Job(runnable, num_total_tasks, i, task_id));
        }
        cv.notify_all();
    } else {
        waiting_tasks.push_back(Task(runnable, num_total_tasks, task_id, deps));
    }

    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [this] { return remaining_tasks == 0; });
    task_remaining_jobs.clear();
}
