#!/usr/bin/env python2.6


###############################################################################
#
# Main
#
if __name__ == '__main__':

    #
    # Runtime characteristics
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


    # M SYSTEMS, G STAGES
    # INPUTS PER SYSTEM, FIRST STAGE ONLY (mineq_coor, mineq_vel, mineq_xsc)
    #     - mineq_coor[M]
    #     - mineq_vel[M]
    #     - mineq_xsc[M]
    #
    input_per_system_first_stage = [ 'mineq_coor', 'mineq_vel', 'mineq_xsc' ]
    #
    # INPUTS PER STAGE FOR ALL SYSTEMS (conf)
    #     - conf_1 .. conf_G
    #
    input_per_stage_all_systems = [ 'conf' ]


    #
    # INPUTS PER SYSTEM FOR ALL STAGES (sys.pdb, sys.parm, sys.crc)
    #     - pdb[M]
    #     - parm[M]
    #     - crc[M]
    #
    input_per_system_all_stages = [ 'pdb', 'parm', 'crc' ]


    #
    # SINKS PER SYSTEM PER STAGE (dcd, cvd, xst, out, err)
    #     - dcd_1[M] .. dcd_G[M]
    #     - cvd_1[M] .. cvd_G[M]
    #     - xst_1[M] .. xst_G[M]
    #     - out_1[M] .. out_G[M]
    #     - err_1[M] .. err_G[M]
    #
    sink_per_system_per_stage = [ 'dcd', 'cvd', 'xst', 'out', 'err' ]


    #
    # INTERMEDIATE PER SYSTEM PER STAGE (coor, vel, xsc)
    #     - coor_1[M] .. coor_G[M]
    #     - vel_1[M] .. vel_G[M]
    #     - xsc_1[M] .. xsc_G[M]
    #
    intermediate_per_system_per_stage = [ 'coor', 'vel', 'xsc' ]


    #
    # SINKS PER SYSTEM FOR FINAL STAGE ONLY (coor, vel, xsc)
    #     - coor[M]
    #     - vel[M]
    #     - xsc[M]
    #
    sink_per_system_final_stage = [ 'coor', 'vel', 'xsc' ]
    #

#
###############################################################################


