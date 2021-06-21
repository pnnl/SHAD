Productive Programming of Distributed Systems with the SHAD C++ Library
****
Monday 21 June 2021, 13:00 - 16:00 EDT
****
Agenda
****
| 13:00 - 13:15: Introduction and Setup
| 13:15 - 14:00: SHAD Library Overview
| 14:00 - 14:45: Runtime and Tasking + Hands on
| 14:45 - 15:00: Break
| 15:00 - 15:55: Data Strcutures and Algorithms + Hands On
| 15:55 - 16:00: Concluding Remarks


****
Getting Started
****
Get the tutorial docker files from github

.. code-block:: shell

   $ git clone https://github.com/VitoCastellana/shad-tutorial-cluster
   $ cd shad-tutorial-cluster
 
Create our tutorial docker cluster

.. code-block:: shell

   $ docker compose build
   $ docker compose up -d --scale node=4
   $ bash ./gen_machines_list.sh
   $ ssh -p 2022 -i ssh/id_rsa.mpi tutorial@localhost

The container prompt should appear:

.. code-block:: shell

   $ mpiexec -n 4 -pernode --hostfile ~/machines hostname
   $ mpiexec --pernode -n 4 -pernode --hostfile ~/machines ./SHAD-build/examples/pi/pi --gmt_num_workers 3
