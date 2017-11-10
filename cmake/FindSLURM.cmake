find_program(SLURM_SBATCH_COMMAND sbatch DOC "Path to the SLURM sbatch executable")
find_program(SLURM_SALLOC_COMMAND salloc DOC "Path to the SLURM salloc executable")
find_program(SLURM_SRUN_COMMAND srun DOC "Path to the SLURM srun executable")

mark_as_advanced(SLURM_SBATCH_COMMAND SLURM_SRUN_COMMAND SLURM_SALLOC_COMMAND)

if (SLURM_SBATCH_COMMAND AND SLURM_SRUN_COMMAND AND SLURM_SALLOC_COMMAND)
  set(SLURM_FOUND true)
  message(STATUS "Found SLURM.")
else()
  set(SLURM_FOUND false)

  if (SLURM_FIND_REQUIRED)
    message(FATAL_ERROR "Could not find SLURM.")
  else()
    message(STATUS "Could not find SLURM.")
  endif()
endif()
