# Memory Assignment
There are several ways by which this algorithm tries to optimally use the available memory on memory constraint tasks, while also not wasting CPU resources.


## Preliminaries
First it is important to note that for the tokenizer stage, angr will use a lot of memory. The bigger the input file, the longer it will take and the more memory it will use. However, it does not use all memory at once, but gradually increase till reaching the peak shortly before starting tokenization. 

### Opportunistic Work (Memory) Stealing
During the time we are yet to reach the peak, be can perform memory stealing as an optimisation, by running disassembly on smaller binaries that thus use less memory and finish before the peak is reached.

### Peak Estimation
The peak memory usage is estimated by a best fit of a power-law curve to the empirical data. 

### Assignment algorithm
We use a simple but suboptimal greedy algorithm to assign tasks to workers preferring larger binaries as long as the estimated memory usage fits within the memory limit.

### Mitigating greedy problems
As we have binaries of various sizes using many workers may quickly exhaust the memory leaving us with no work assignable to the remaining workers. Generally, this is fine as smaller binaries finish much quicker, so overall we must finish the large binaries first. 
However, we do still want to try to use all CPUs available, so we assign each subsequent worker a cutoff budget that is a fraction of the max-memory with the goal of spreading the work somewhat evenly over all binary sizes.

## Implementation
### 1. Initial assignment
The first workers budget is the maximum memory limit. The second gets half, The third gets a quarter. However from the forth onwards we dont half the budget, instead the forths get a fifth, the fifths get a sixth, and so on. Further, besides for the first we add 150MB to each budget.
The inital assignment assigned budget is special, because even after the task completes the worker will always be assumed to have at this budget. 

### 2. Marking as opportunistic
During the initial assignment we strictly keep track of how much memory we have assigned of the memory limit. If based on the budget we gave the task we would exceed the memory limit we mark the task as opportunistic. This marks the task as memory stealing, and we kill them preemptively, if we are about to exceed the memory limit (See OOM management). (all workers currently use - 500MB or limit/cores whichever is smaller - less then the memory limit)

Marking a task as opportunistic is permanent, and the worker will stay marked for the whole life of the manager. 

### 3. Completing tasks
When a task finishes successfully, errors, or crashes, the worker will be restarted if down or if --always-restart-workers is set. After this it is marked as idle.
If a task errored or crashed, the task is added the errored queue.

### 4. Assigning new tasks
If there are idle tasks we look at the current total memory usage, ignoring the usage of the idle tasks. With that we compute the available memory = memory limit - total memory usage. Here we measure the actual memory usage, we do not use the estimate!
1. we order the idle workers by their budget
2. each worker is assigned a secondary 'temporary budget factor' 1st idle worker 1.5, 2nd is 2, 3rd is 3, etc.
3. if the worker is not opportunistic, we assign a task only based on the initial budget, ignoring the temporary budget
4. now starting with the smallest opportunistic worker we assign a task based on the smaller budget: initial vs available / temporary budget factor
5. the memory used based on the estimate is substracted from available memory. (please note that for non-opportunistic workers we do not subtract)
6. if we fail to assign bacause no estimate fits, we add it to a list to handle in the next step.
7. after we have done step 3 to 6 for all we continue with step 8
8. we try again only with the failed workers from step 1. once only
9. if we fail after another iteration, we add keep it idle. the first time an idle worker stays idle after having completed (successfully or not) a task, we log this 

### 5. Finishing up
If there are no tasks left that fit into any workers, we add them to the OOM queue.

### OOM management
The OOM manager will start killing workers if all workers currently use - 500MB or limit/cores whichever is smaller - less then the memory limit. If this is the case we kill the median opportunistic worker.
If there are no opportunistic workers, we wait till the memory limit is exceeded, and then kill the smallest worker and mark is permanently as opportunistic.
If worker first (the one with the largest budget - worker 0) would be killed, we do not mark it as opportunistic, and we do not requeue the task, instead we add the task to the OOM queue.

Killed workers are restarted, marked as idle, and the task is requeued.


# Managing Errored tasks
As stated all errored tasks are added to the errored queue. After the initial queue finishes, we run again with the errored queue, by setting the normal queue to the errored queue and clearing the errored queue. If we complete this again and there are still errored tasks, we report them as failed when the manager is shutting down. 

# Managing OOM tasks
We stop all but one worker. We now startr with the smallest task in the OOM queue, and run it on the worker regardless of if we have the budget. If we again kill a task for OOM we do not try it again, and report the task as OOM when the manager is shutting down.
