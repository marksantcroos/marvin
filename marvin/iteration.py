__author__ = 'mark'

################################################################################
#
#
# Inputs have (name, type)
# Constants have (name, type, value, cardinality)
# Cardinality can be (singleton, scalar)
# Singleton is set with one element, can be used in depth > 0
# Scalar is single value, can only be used with depth = 0
# Input ports have (name, type, depth)
# Output ports have (name, type, depth)
# Type can be (String, Integer, Double, URI)
#

#
###############################################################################

import itertools



def flat_cross_product(self, ports):
    """ The flat-cross product is not to be used as an iteration strategy as it only differs from the "normal" cross product in the way it produces the output. """

    cross = self.cross_product(ports)

    flat = [j for i in cross for j in i]

    print flat

    print 'Number of iterations', len(flat)

    return flat


def cross_product(self, ports):
    """ This iteration strategy creates a cross product out of all values on the input ports. """

    print 'Number of ports:', len(ports)
    print 'Max depth:', max(len(p) for p in ports)

    cross = [[]]
    for x in ports:
        cross = [ i + [y] for y in x for i in cross ]
    print 'Cross:', cross

    print 'Number of iterations', len(cross)

    return cross

def dot_product(self, ports):
    """ This iteration strategy creates a dot product out of all values on the input ports. """

    num_ports = len(ports)
    print 'Number of ports:', num_ports
    max_depth = max(len(p) for p in ports)
    print 'Max depth:', max_depth

    for p in ports:
        if len(p) != max_depth:
            raise('Oops, not all ports have the same length, not possible with dot product!')

    r = [[ports[i][j] for i in range(num_ports) ] for j in range(max_depth)]

    print 'Number of iterations', len(r)

    print r

    return r


###############################################################################
def print_tree(wx):

    def print_iter(p_iter, indent):

        if p_iter.i_type == 'ITERATION':
            print indent + 'Iteration:', p_iter.i_strat
        elif p_iter.i_type == 'PORT':
            print indent + 'Port:', p_iter.i_port

        if p_iter.i_child:
            print_iter(p_iter.i_child, indent + '  ')
        if p_iter.i_next:
            print_iter(p_iter.i_next, indent)


    print '  Processors:'
    for p in wx.workflow.processors:
        print '    Processor:', p.p_name

        print '      GASW:', p.p_gasw.g_desc
        for i in p.p_in:
            print '      Input:', i.i_name, i.i_type, i.i_depth
        for o in p.p_out:
            print '      Output:', o.o_name, o.o_type, o.o_depth

        if p.p_iter:
            print_iter(p.p_iter, '      ')
##############################################################################


###############################################################################
#
# Main
#
if __name__ == '__main__':


    gwendia_file = '../examples/iter-test.gwendia'
    #gwendia_file = '../examples/dti_bedpost.gwendia'


    from workflow_xml import WorkflowXML
    wx = WorkflowXML()
    wx.read_from_file(gwendia_file)
    #wx.text_out()

    print_tree(wx)
