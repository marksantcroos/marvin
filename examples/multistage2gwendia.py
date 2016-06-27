#!/usr/bin/env python2.6

import time
from marvin.abstractwf import AbstractWF, Sink, Processor, Port, Source

# Input to step X:
# - dynX-1.coor, dynX-1.vel, dynX-1.xsc (passed from previous step): 7.2MB
# - dynX.conf (same for all systems, one for each ns MD step)
# - sys.pdb, sys.crd, sys.parm: 33.6MB
# Output from step X :
# - dynX.coor, dynX.vel, dynX.xsc: passed to next step
# - dynX.out, dynX.err (diagnostic information): 167MB
# - dynX.dcd, dynX.cvd, dynX.xst (used for further analysis): 3.6GB
def ms2a(systems, steps):

    awf = AbstractWF()

    # Source nodes per system
    # - sys.pdb, sys.crd, sys.parm: 33.6MB
    system_source_ext_list = ['pdb', 'crd', 'parm']
    for ext in system_source_ext_list:
        node = Source()
        node.name = 'sys_%s' % (ext)
        node.label = node.name
        awf.add_node(node.name, node)

    # Source per system for first step
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

    # Loop over all STEPS
    for step in range(steps):

        # Source per STEP (config)
        # - dynX.conf (same for all systems, one for each ns MD step)
        ext = 'conf'
        node = Source()
        node.name = 'step%s_%s' % (step, ext)
        node.label = node.name
        # As all systems share the same config for every step,
        # we only need to create it once per step.
        if not awf.exist_node(node.name):
            awf.add_node(node.name, node)

        # Sinks per step
        stage_sink_ext_list = ['out', 'err', 'dcd', 'cvd', 'xst']
        for ext in stage_sink_ext_list:
            node = Sink()
            node.name = 'step%s_%s' % (step, ext)
            node.label = node.name
            awf.add_node(node.name, node)

        # The actual process!
        proc = Processor()
        proc.label = 'step%s' % (step)
        proc.name = 'step%s' % (step)
        awf.add_node(proc.name, proc)

        # - dynX-1.coor, dynX-1.vel, dynX-1.xsc (passed from previous step): 7.2MB
        stage_in_ext_list = ['coor', 'vel', 'xsc' ]
        for ext in stage_in_ext_list:
            port = Port()
            port.label = 'IN_' + ext
            port.name = '%s:%s' % (proc.name, port.label)
            awf.add_node(port.name, port)

            # Add connection from input port to node
            awf.add_edge(port.name, proc.name)

            if step == 0:
                # Add connection from source to port
                awf.add_edge('mineq_%s' % (ext), port.name)
            else:
                # Add connection from source to port
                awf.add_edge('step%s:OUT_%s' % (step -1, ext), port.name)


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
        # - dynX.conf (same for all systems, one for each ns MD step)
        ext = 'conf'
        port = Port()
        port.label = 'IN_' + ext
        port.name = '%s:%s' % (proc.name, port.label)
        awf.add_node(port.name, port)

        # Add connection from input port to node
        awf.add_edge(port.name, proc.name)

        # Add connection from source to port
        awf.add_edge('step%s_%s' % (step, ext), port.name)

        # Output from step X :
        # - dynX.coor, dynX.vel, dynX.xsc: passed to next step
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

            # Add connection from output port to sink for last step
            if step == steps - 1:
                awf.add_edge(port.name, '%s' % (ext))

        for ext in diag_ext_list + ana_ext_list:
            port = Port()
            port.label = 'OUT_' + ext
            port.name = '%s:%s' % (proc.name, port.label)
            awf.add_node(port.name, port)

            # Add connection from output node to port
            awf.add_edge(proc.name, port.name)

            # Add connection from output port to sink
            awf.add_edge(port.name, 'step%s_%s' % (step, ext))

        #
        # End of STEP loop
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

    awf = ms2a(NUM_SYSTEMS, NUM_STAGES)
    #awf.nx_draw()
    awf.pg_draw()

    print 'EOF'
#
###############################################################################


