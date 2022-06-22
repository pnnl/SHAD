**************************************
A Docker cluster for tutorials on SHAD
**************************************

.. code-block:: shell
   # if docker compose is not available use docker-compose
   $ docker compose build
   $ docker compose up -d --scale node=4
   $ bash ./gen_machines_list.sh
   $ ssh -p 2022 -i ssh/id_rsa.mpi tutorial@localhost

The container prompt should appear:

.. code-block:: shell

   $ mpiexec -n 4 -pernode --hostfile machines hostname
   $ mpiexec -n 4 -pernode --hostfile machines ./SHAD-build/examples/pi/pi --gmt_num_workers 4
