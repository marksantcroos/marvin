import time
from marvin.abstractwf import AbstractWF, Sink, Processor, Port, Source


def exp2gwendia(num_tasks, num_steps, num_inputs, num_outputs):

    awf = AbstractWF()

    for task in range(num_tasks):

        # Sinks (outputs) per task
        system_sink_ext_list = ['out-%d' % x for x in range(10)]
        for ext in system_sink_ext_list:
            node = Sink()
            node.name = '%s' % (ext)
            node.label = node.name
            awf.add_node(node.name, node)

    return awf

###############################################################################
#
# Main
#
if __name__ == '__main__':

    #
    # Runtime characteristics
    #
    # Number of resources
    NUM_RESOURCES = 1
    # Number of cores per resource
    NUM_CORES = 5
    # The number of initial tasks
    NUM_TASKS = 10
    # The number of steps
    NUM_STEPS = 10
    # The number of outputs each task creates
    NUM_OUTPUTS = 1
    # The output file size
    OUTPUT_SIZE = 1
    # The number of inputs each task requires
    NUM_INPUTS = 2

    awf = exp2gwendia(NUM_TASKS, NUM_STEPS, NUM_INPUTS, NUM_OUTPUTS)
    awf.pg_draw('exp_gwendia')

    print 'EOF'
#
###############################################################################

