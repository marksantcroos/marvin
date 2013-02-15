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
from workflow_xml import WorkflowXML


###############################################################################
def build_tree(wx):

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
    wx = WorkflowXML()
    wx.read_from_file(gwendia_file)
    #wx.text_out()

    build_tree(wx)
