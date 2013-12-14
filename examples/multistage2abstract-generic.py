#!/usr/bin/env python2.6

import time
from marvin.abstractwf import AbstractWF, Sink, Processor, Port, Source

# Input to stage X:
# - dynX-1.coor, dynX-1.vel, dynX-1.xsc (passed from previous stage): 7.2MB
# - dynX.conf (same for all systems, one for each ns MD stage)
# - sys.pdb, sys.crd, sys.parm: 33.6MB
# Output from stage X :
# - dynX.coor, dynX.vel, dynX.xsc: passed to next stage
# - dynX.out, dynX.err (diagnostic information): 167MB
# - dynX.dcd, dynX.cvd, dynX.xst (used for further analysis): 3.6GB
def ms2a(systems, stages):

    awf = AbstractWF()

    # Source nodes per system
    # - sys.pdb, sys.crd, sys.parm: 33.6MB
    system_source_ext_list = ['pdb', 'crd', 'parm']
    for ext in system_source_ext_list:
        node = Source()
        node.name = 'sys_%s' % (ext)
        node.label = node.name
        awf.add_node(node.name, node)

    # Source per system for first stage
    system_sink_ext_list = ['coor', 'vel', 'xsc']
    for ext in system_sink_ext_list:
        node = Source()
        node.name = 'mineq_%s' % (ext)
        node.label = node.name
        awf.add_node(node.name, node)

    # Sinks per system
    system_sink_ext_list = ['coor', 'vel', 'xsc']
    for ext in system_sink_ext_list:
        node = Sink()
        node.name = '%s' % (ext)
        node.label = node.name
        awf.add_node(node.name, node)

    # Loop over all STAGES
    for stage in range(stages):

        # Source per STAGE (config)
        # - dynX.conf (same for all systems, one for each ns MD stage)
        ext = 'conf'
        node = Source()
        node.name = 'stage%s_%s' % (stage, ext)
        node.label = node.name
        # As all systems share the same config for every stage,
        # we only need to create it once per stage.
        if not awf.exist_node(node.name):
            awf.add_node(node.name, node)

        # Sinks per stage
        stage_sink_ext_list = ['out', 'err', 'dcd', 'cvd', 'xst']
        for ext in stage_sink_ext_list:
            node = Sink()
            node.name = 'stage%s_%s' % (stage, ext)
            node.label = node.name
            awf.add_node(node.name, node)

        # The actual process!
        proc = Processor()
        proc.label = 'stage%s' % (stage)
        proc.name = 'stage%s' % (stage)
        awf.add_node(proc.name, proc)

        # - dynX-1.coor, dynX-1.vel, dynX-1.xsc (passed from previous stage): 7.2MB
        stage_in_ext_list = ['coor', 'vel', 'xsc' ]
        for ext in stage_in_ext_list:
            port = Port()
            port.label = 'IN_' + ext
            port.name = '%s:%s' % (proc.name, port.label)
            awf.add_node(port.name, port)

            # Add connection from input port to node
            awf.add_edge(port.name, proc.name)

            if stage == 0:
                # Add connection from source to port
                awf.add_edge('mineq_%s' % (ext), port.name)
            else:
                # Add connection from source to port
                awf.add_edge('stage%s:OUT_%s' % (stage -1, ext), port.name)


        # - sys.pdb, sys.crd, sys.parm: 33.6MB
        sys_in_ext_list = ['pdb', 'crd', 'parm']
        for ext in sys_in_ext_list:
            port = Port()
            port.label = 'IN_' + ext
            port.name = '%s:%s' % (proc.name, port.label)
            awf.add_node(port.name, port)

            # Add connection from input port to node
            awf.add_edge(port.name, proc.name)

            # Add connection from source to port
            awf.add_edge('sys_%s' % (ext), port.name)

        # Config port
        # - dynX.conf (same for all systems, one for each ns MD stage)
        ext = 'conf'
        port = Port()
        port.label = 'IN_' + ext
        port.name = '%s:%s' % (proc.name, port.label)
        awf.add_node(port.name, port)

        # Add connection from input port to node
        awf.add_edge(port.name, proc.name)

        # Add connection from source to port
        awf.add_edge('stage%s_%s' % (stage, ext), port.name)

        # Output from stage X :
        # - dynX.coor, dynX.vel, dynX.xsc: passed to next stage
        stage_out_ext_list = ['coor', 'vel', 'xsc']
        # - dynX.out, dynX.err (diagnostic information): 167MB
        diag_ext_list = ['out', 'err']
        # - dynX.dcd, dynX.cvd, dynX.xst (used for further analysis): 3.6GB
        ana_ext_list = ['dcd', 'cvd', 'xst']

        for ext in stage_out_ext_list:
            port = Port()
            port.label = 'OUT_' + ext
            port.name = '%s:%s' % (proc.name, port.label)
            awf.add_node(port.name, port)

            # Add connection from output node to port
            awf.add_edge(proc.name, port.name)

            # Add connection from output port to sink for last stage
            if stage == stages - 1:
                awf.add_edge(port.name, '%s' % (ext))

        for ext in diag_ext_list + ana_ext_list:
            port = Port()
            port.label = 'OUT_' + ext
            port.name = '%s:%s' % (proc.name, port.label)
            awf.add_node(port.name, port)

            # Add connection from output node to port
            awf.add_edge(proc.name, port.name)

            # Add connection from output port to sink
            awf.add_edge(port.name, 'stage%s_%s' % (stage, ext))

        #
        # End of STAGE loop
        #

    return awf

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
    # INPUTS PER SYSTEM FOR FIRST STAGE ONLY (mineq_coor, mineq_vel, mineq_xsc)
    #     - mineq_coor[M]
    #     - mineq_vel[M]
    #     - mineq_xsc[M]
    # INPUTS PER STAGE FOR ALL SYSTEMS (conf)
    #     - conf_1 .. conf_G
    # INPUTS PER SYSTEM FOR ALL STEPS (sys.pdb, sys.parm, sys.crc)
    #     - pdb[M]
    #     - parm[M]
    #     - crc[M]
    # SINKS PER SYSTEM PER STEP (dcd, cvd, xst, out, err)
    #     - dcd_1[M] .. dcd_G[M]
    #     - cvd_1[M] .. cvd_G[M]
    #     - xst_1[M] .. xst_G[M]
    #     - out_1[M] .. out_G[M]
    #     - err_1[M] .. err_G[M]
    # INTERMEDIATE PER SYSTEM PER STEP (coor, vel, xsc)
    #     - coor_1[M] .. coor_G[M]
    #     - vel_1[M] .. vel_G[M]
    #     - xsc_1[M] .. xsc_G[M]
    # SINKS PER SYSTEM FOR FINAL STEP ONLY (coor, vel, xsc)
    #     - coor[M]
    #     - vel[M]
    #     - xsc[M]


    awf = ms2a(NUM_SYSTEMS, NUM_STAGES)
    #awf.nx_draw()
    awf.pg_draw()

    print 'EOF'
#
###############################################################################


