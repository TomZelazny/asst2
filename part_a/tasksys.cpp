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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    std::thread threads[num_threads];
    std::vector<int> task_ids;
    std::mutex task_ids_mutex;
    for (int i = 0; i < num_total_tasks; i++) {
        task_ids.push_back(i);
    }
    for (int i = 0; i < num_threads; i++) {
        threads[i] = std::thread([&task_ids, &task_ids_mutex, runnable, num_total_tasks] {
            while (true) {
                int task_id = -1;
                {
                    std::lock_guard<std::mutex> lock(task_ids_mutex);
                    if (!task_ids.empty()) {
                        task_id = task_ids.back();
                        task_ids.pop_back();
                    }
                }
                if (task_id != -1) {
                    runnable->runTask(task_id, num_total_tasks);
                } else {
                    break;
                }
            }
        });
    }
    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */
Task::Task(IRunnable* runnable, int num_total_tasks, int task_id): runnable(runnable), num_total_tasks(num_total_tasks), task_id(task_id) {}

Task::~Task() {}

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    // std::printf("num_threads: %d\n", num_threads);
    stop = false;
    task_remainings = 0;
    for (int i = 0; i < num_threads; i++) {
        thread_pool.push_back(std::thread([this] {
            while (true) {
                Task task(nullptr, 0, 0);
                {
                    std::lock_guard<std::mutex> lock(task_queue_mutex);
                    if (!task_queue.empty()) {
                        task = task_queue.front();
                        task_queue.pop();
                    }
                }
                if (task.runnable != nullptr) {
                    task.runnable->runTask(task.task_id, task.num_total_tasks);
                    task_remainings--;
                } else if (stop && task_queue.empty()) {
                    break;
                }
            }
        }));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    stop = true;
    for (int i = 0; i < thread_pool.size(); i++) {
        thread_pool[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    task_remainings = num_total_tasks;
    {
        std::lock_guard<std::mutex> lock(task_queue_mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue.push(Task(runnable, num_total_tasks, i));
        }
    }
    while (true) {
        if (task_remainings == 0) {
            break;
        }
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    stop = false;
    task_remainings = 0;
    for (int i = 0; i < num_threads; i++) {
        thread_pool.push_back(std::thread([this] {
            while (true) {
                Task task(nullptr, 0, 0);
                {
                    std::unique_lock<std::mutex> lock(task_queue_mutex);
                    cv.wait(lock, [this] { return !task_queue.empty() || stop; });
                    if (stop && task_queue.empty()) {
                        break;
                    }
                    if (!task_queue.empty()) {
                        task = task_queue.front();
                        task_queue.pop();
                    }
                }
                if (task.runnable != nullptr) {
                    task.runnable->runTask(task.task_id, task.num_total_tasks);
                    // {
                    //     std::lock_guard<std::mutex> lock(task_queue_mutex);
                    //     task_remainings--;
                    // }
                    // cv.notify_all();
                    if (task_remainings.fetch_sub(1) == 1) {
                        cv.notify_all();
                    }
                }
            }
        }));
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
    for (int i = 0; i < thread_pool.size(); i++) {
        thread_pool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    task_remainings = num_total_tasks;
    {
        std::lock_guard<std::mutex> lock(task_queue_mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue.push(Task(runnable, num_total_tasks, i));
        }
    }
    cv.notify_all();
    
    std::unique_lock<std::mutex> lock(task_queue_mutex);
    cv.wait(lock, [this] { return task_remainings == 0; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
