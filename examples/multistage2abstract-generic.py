#!/usr/bin/env python2.6


class MultiTaskMultiStage():

    def __init__(self):

        tasks = []
        stages = []

        task_executable = None

        input_per_task_first_stage = []
        input_all_tasks_per_stage = []
        input_per_task_all_stages = []
        output_per_task_per_stage = []
        intermediate_per_task_per_stage = []
        output_per_task_final_stage = []

    def run(self):
        pass


###############################################################################
#
# Main
#
if __name__ == '__main__':

    #
    # Application specific runtime characteristics
    #
    # Number of chromosomes
    NUM_CHRS = 1 # exp:5
    # Number of locations per chromosome
    NUM_LOCS = 2 # exp:21
    # Total number of systems to model, paper uses 105
    NUM_SYSTEMS = NUM_CHRS * NUM_LOCS
    # The time of simulation per system
    SIM_TRAJ_TIME = 3 # exp:20
    # The simulation time per task
    TASK_SIM_TIME = 1
    # The number of stages per system
    NUM_STAGES = SIM_TRAJ_TIME / TASK_SIM_TIME
    # Total number of tasks to execute
    #NUM_TASKS = NUM_STAGES * NUM_SYSTEMS

    mtms = MultiTaskMultiStage()
    mtms.tasks = range(NUM_SYSTEMS)
    mtms.stages = range(NUM_STAGES)
    mtms.task_executable = 'namd-script.sh'

    #
    # M SYSTEMS, G STAGES
    # INPUTS PER SYSTEM, FIRST STAGE ONLY (mineq_coor, mineq_vel, mineq_xsc)
    #     - mineq_coor[M]
    #     - mineq_vel[M]
    #     - mineq_xsc[M]
    #
    #
    mtms.input_per_task_first_stage = [
        'mineq-{TASK}.coor',
        'mineq-{TASK}.vel',
        'mineq-{TASK}.xsc'
    ]

    #
    # INPUTS PER STAGE FOR ALL SYSTEMS (conf)
    #     - conf_1 .. conf_G
    #
    mtms.input_all_tasks_per_stage = [
        'dyn-{STAGE}.conf'
    ]

    #
    # INPUTS PER SYSTEM FOR ALL STAGES (sys.pdb, sys.parm, sys.crc)
    #     - pdb[M]
    #     - parm[M]
    #     - crc[M]
    #
    mtms.input_per_task_all_stages = [
        'sys-{TASK}.pdb',
        'sys-{TASK}.parm',
        'sys-{TASK}.crc'
    ]

    #
    # SINKS PER SYSTEM PER STAGE (dcd, cvd, xst, out, err)
    #     - dcd_1[M] .. dcd_G[M]
    #     - cvd_1[M] .. cvd_G[M]
    #     - xst_1[M] .. xst_G[M]
    #     - out_1[M] .. out_G[M]
    #     - err_1[M] .. err_G[M]
    #
    mtms.output_per_task_per_stage = [
        'dyn-{TASK}-{STAGE}.dcd',
        'dyn-{TASK}-{STAGE}.cvd',
        'dyn-{TASK}-{STAGE}.xst',
        'dyn-{TASK}-{STAGE}.out',
        'dyn-{TASK}-{STAGE}.err'
    ]

    #
    # INTERMEDIATE PER SYSTEM PER STAGE (coor, vel, xsc)
    #     - coor_1[M] .. coor_G[M]
    #     - vel_1[M] .. vel_G[M]
    #     - xsc_1[M] .. xsc_G[M]
    #
    mtms.intermediate_per_task_per_stage = [
        'dyn-{TASK}-{STAGE}.coor',
        'dyn-{TASK}-{STAGE}.vel',
        'dyn-{TASK}-{STAGE}.xsc'
    ]

    #
    # OUTPUTS PER SYSTEM FOR FINAL STAGE ONLY (coor, vel, xsc)
    #     - coor[M]
    #     - vel[M]
    #     - xsc[M]
    #
    mtms.output_per_task_final_stage = [
        'dyn-{TASK}-{STAGE}.coor',
        'dyn-{TASK}-{STAGE}.vel',
        'dyn-{TASK}-{STAGE}.xsc'
    ]

#
###############################################################################
