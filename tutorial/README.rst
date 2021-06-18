Productive Programming of Distributed Systems with the SHAD C++ Library
****
Friday June 18 2021, 10:00 AM - 2:00 PM EDT
****
Agenda
****
| 10:00 - 10:15 : Introduction and Setup
| 10:15 - 11:00 : SHAD Library Overview
| 11:00 - 11:15 : Break
| 11:15 - 12:00 : Runtime and Tasking + Hands on
| 12:00 - 12:45 : Data Structures, Extensions and Utilities + Hands on
| 12:45 - 1:00  : Break
| 1:00  - 1:55  : Iterators and  Algorithms + Hands On
| 1:55  - 2:00  : Concluding Remarks

****
Getting Started
****
Get the tutorial docker files from github

.. code-block:: shell

   $ git clone https://github.com/mminutoli/shad-tutorial-cluster.git
   $ cd shad-tutorial-cluster
 
Create our tutorial docker cluster

.. code-block:: shell

   $ docker compose build
   $ docker compose up -d --scale node=4
   $ bash ./gen_machines_list.sh
   $ ssh -p 2022 -i ssh/id_rsa.mpi tutorial@localhost

The container prompt should appear:

.. code-block:: shell

   $ mpiexec -n 4 -pernode --hostfile machines hostname
   $ mpiexec --pernode -n 4 -pernode --hostfile machines ./SHAD-build/examples/pi/pi --gmt_num_workers 3