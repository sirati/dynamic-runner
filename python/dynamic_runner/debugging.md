# Debugging test commands

These are increasingly difficult, starting with the simplest and progressing to more complex scenarios.

For debugging, always run **exactly** as shown, without **any** modifications.

The first parameter sets the timeout, the seconds parameter sets the tail lines of output that are to be displayed. The numbers here should be correct for useful output.

### Relevant Information of correct implementation
for worker manager oop structure /dynamic_batch/worker_manager/oop.md
for memory management /dynamic_batch/memory_assignment.md
for primary secondary fundamentals: /dynamic_batch/multi_computer/slurm_original_instructions.txt

---

## Base usage

```bash
./debug.sh 10s 100
````
Checklist:
- Only core number of tasks are assigned in initial phase
- Main phase
- Errored tasks are restarted if error type non_recoverable
- Workers of errored talks are assigned now taks
- Errored tasks are retried one in the retry phase
- Workers are not shut down in main phase, if all tasks are finished but the queue for retrt is not empty
- Do not restart workers needlessly: e.g. at start of a phase

```bash
./debug.sh 15s 100 --test-master-slave
```
Checklist:
- Same as above
- We do not hang
- Same output as above, but some log messages are dublicated (expected and correct)

```bash
./debug.sh 15s 100 --test-master-slave-netsim
```

---

## Local debugging for slurm by not using slurm

```bash
./debug.sh 30s 150 --multi-computer single-process --jobs 1 --cores 16
```

```bash
./debug.sh 30s 150 --multi-computer local --jobs 1 --cores 16
```

---

## Also testing inter-secondary communication
Checklist:
- Secondaries build peer to peer network
- Both secondaries get an initial assignment up to their worker count
- One secondary is promoted to slurm-primary and the other is aware of that
- The secondary asks for new assignments from the slurm-primary 


```bash
./debug.sh 30s 200 --multi-computer single-process --jobs 2 --cores 8
```

```bash
./debug.sh 30s 200 --multi-computer local --jobs 2 --cores 8
```

---

## Endboss
Using slurm takes about 2 minutes before secondaries are starting!
```bash
./debug.sh 240s 300 --multi-computer slurm --jobs 2 --cores 8
```
