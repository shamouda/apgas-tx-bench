# ResilientTxBench
ResilientTxBench is a java benchmark for evaluating Hazelcast transaction performance.
It uses the APGAS[1] library as a means for creating asynchronous tasks and distributed processes.
The design of the benchmark is influenced by Synchrobench (https://github.com/gramoli/synchrobench), and its extension used in [2].
 
References:
[1] Tardieu, Olivier. "The APGAS library: Resilient parallel and distributed programming in Java 8." Proceedings of the ACM SIGPLAN Workshop on X10. ACM, 2015. 
[2] Chapman, Keith, Antony L. Hosking, and J. Eliot B. Moss. "Hybrid STM/HTM for nested transactions on OpenJDK." Proceedings of the 2016 ACM SIGPLAN International Conference on Object-Oriented Programming, Systems, Languages, and Applications. ACM, 2016.