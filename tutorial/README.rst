Boosting Productivity and Applications Performance on Parallel Distributed Systems with the SHAD C++ Library
****
October 8 2022, 14:15 - 17:45 CDT
****
Vito Giovanni Castellana, Marco Minutoli, Nanmiao Wu, John Feo
****
| In this tutorial, we first overview the design of the SHAD library, depicting its main components: runtime systems abstractions for tasking; parallel and distributed data-structures; STL-compliant interfaces and algorithms.  The tutorial features interactive hands-on sessions, with coding exercises covering the different components of the software, from the tasking API up to the STL algorithms and Data Structures layers. Finally, we demonstrate how SHAD is used to develop complex applications, at scale.
****
Agenda
****
| 14:15 - 14:30: Introduction and Setup
| 14:30 - 15:00: SHAD Library Overview
| 15:00 - 15:45: Runtime and Tasking + Hands on
| 15:45 - 16:15: Coffee Break
| 16:15 - 17:00: Data Structures and Algorithms + Hands On
| 17:00 - 17:35: Extensions and Advanced Features
| 17:35 - 17:45: Concluding Remarks
****
About SHAD
****
| High-performance computing (HPC) is often perceived as a matter of making large-scale systems (e.g., clusters) run as fast as possible, regardless the required programming effort. However, the idea of "bringing HPC to the masses" has recently emerged.  Inspired by this vision, we have designed SHAD, the Scalable High-performance Algorithms and Data-structures library. SHAD is open source software, written in C++, for C++ developers. Unlike other HPC libraries for distributed systems, which rely on SPMD models, SHAD adopts a shared-memory programming abstraction, to make C++ programmers feel at home. Underneath, SHAD manages tasking and data-movements, moving the computation where data resides and taking advantage of asynchrony to tolerate network latency.
At the bottom of his stack, SHAD can interface with multiple runtime systems: this not only improves developer’s productivity, by hiding the complexity of such software and of the underlying hardware, but also greatly enhance code portability. Thanks to its abstraction layers, SHAD can indeed target different systems, ranging from laptops to HPC clusters, without any need for modifying the user-level code.
