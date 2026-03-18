WorkerManagerBase is the ABC of all WorkerManagerMethods as using by the normal execution
Now there are three types of WorkerManager: Authoritative, Submissive, and Local
Local: Does everything
Authoritative: Makes any decisions but time-critical ones (decides initial assignment, marking as opportunistic, idle worker managment, assignment to new tasks)
Submissive: Does actual management and execution, and time-critical decisions (time-critical: OOM management, but cannot mark as opportunistic, but has to report worker error to Authoritative) (management and execution: it starts, restarts, kills workers and communicates with them)

Now there is some issues with the OOP structure. submissive and authoritative work in a M:1 relationship. But we need a object to represent the other side that my be behind a network. thats why we again split authoritative and submissive into two subflavour actual (so far called local as in LocalSubmissiveWorkerManager now ActualSubmissiveWorkerManager)) and remote (an object that does nothing besides relaying its method calls over network. Their common methods also exist via a Base

However we notice that we still have the two kinds of responsibility:
1. decisions but time-critical
2. management and execution, time-critical oom decisions

So we have a quite complicated object inheritence structure

WorkerManagerBase
-> DecisionWorkerManBaseImpl (1st responsiblity)
-> ExecutionnWorkerManBaseImpl (2nd responsibility)
-> AuthorativeBase (methods authorative has that WorkerManagerBase doesnt)
-> SubmissiveBase (methods authorative has that WorkerManagerBase doesnt)

LocalWorkerManager: DecisionWorkerManBaseImpl + ExecutionnWorkerManBaseImpl
ActualSubmissiveWorkerManager: ExecutionnWorkerManBaseImpl + SubmissiveBase
RemoteSubmissiveWorkerManager: SubmissiveBase
ActualAuthorativeWorkerManager: DecisionWorkerManBaseImpl + AuthorativeBase
RemoteAuthorativeWorkerManager: AuthorativeBase
