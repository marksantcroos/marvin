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


###############################################################################
#
# Function: iterate_inputs()
#
def iterate_inputs(spec, input):

    ports_from_spec = []
    ports_from_input = input.keys()

    def check_nest(items):
        for item in items:
            if isinstance(item, list):
                check_nest(item)
            elif item in ['ITERATE_DOT', 'ITERATE_CROSS', 'ITERATE_FLAT_CROSS', 'ITERATE_MATCH']:
                pass
            else:
                port = item[0]
                ports_from_spec.append(port)

    check_nest(spec)
    if sorted(ports_from_spec) != sorted(ports_from_input):
        print 'ERROR: Ports provided in input mismatches with iteration strategy!'
        print 'Input provided for port(s):', ports_from_input
        print 'Iteration strategy provided for port(s):', ports_from_spec


    def has_nesting(items):
        for item in items:
            if isinstance(item, list):
                return True
        return False


    def trav_tree(item, result=[]):

#        print 'Item at begin of trav_tree:', item

        if item[0] in ['ITERATE_DOT', 'ITERATE_CROSS', 'ITERATE_FLAT_CROSS', 'ITERATE_MATCH']:
            strat = item[0]
#            print 'Iteration Strategy:', strat
            result.append(strat)
            result = trav_tree(item[1], result)

        elif isinstance(item[0], tuple):
            while True:
                port = item.pop(0)[0]
                values = input[port]
                result.append(values)

#                print 'gathering items:', values

                if not item or not isinstance(item[0], tuple):
                    break

#            print 'item after while:', item

            if item:
                result = trav_tree(item[0], result)

        return result



    #result = trav_tree(spec)
    #print result



    def populate_tree(item, result=[]):

        print 'Item at begin of trav_tree:', item

        if isinstance(item[0], list):
            result = populate_tree(item[0], result)
        if isinstance(item[1], list):
            result = populate_tree(item[1], result)


        if item[0] in ['ITERATE_DOT', 'ITERATE_CROSS', 'ITERATE_FLAT_CROSS', 'ITERATE_MATCH']:
            print 'Iteration Strategy:', item[0]
            print 'Intermedia result:', result

            if item[0] == 'ITERATE_CROSS':
                product = list(itertools.product(*result))
                print 'Product:', product
                result = [product]
            elif item[0] == 'ITERATE_DOT':
                # product = list(itertools.product(*result))
                #product = result
                #result = result *
                print 'result0:', result[0]
                print 'result1:', result[1]
                res = []
                #for x in range(len(result[0])):
                #    res.append((result[0][x],) + result[1][x])
                #result = res
                #result = zip(*result)
                print 'XXX:', result

        while isinstance(item[0], tuple):
            port = item[0][0]
            values = input[port]
            result.insert(0, values)
            print 'Values:', values
            item = item[1]
        if isinstance(item, tuple):
            port = item[0]
            values = input[port]
            result.append(values)
            print 'Values:', values

        return result

    result = populate_tree(spec)

    print 'Result:', result





def build_tree(wx):

    def print_iter(p_iter, indent):

        if p_iter.i_type == 'ITERATION':
            print indent + 'Iteration:', p_iter.i_strat
        elif p_iter.i_type == 'PORT':
            print indent + 'Ports:', p_iter.i_port

        if p_iter.i_next:
            print_iter(p_iter.i_next, indent)
        if p_iter.i_child:
            print_iter(p_iter.i_child, indent + '  ')

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

#    port_depth = 0
#    port_type = 0
#    # dot two ports (left (1,2,3) . right (a,b,c)) = 1a, 2b, 3c
#    it1 = [ 'ITERATE_DOT', [ ('left', port_type, port_depth), ('right', port_type, port_depth) ] ]
#    input1 = { 'left': ['1', '2', '3'],
#               'right': ['a', 'b', 'c'] }
#    # nested dot (left (1,2,3) dot ( right1 (a,b,c) dot right2 (x,y,z) ) =>
#    #                      (1,2,3 . (ax, by, cz)) => 1ax, 2by, 3cz
#    it2 = [ 'ITERATE_DOT', [ ('left', port_type, port_depth), [ 'ITERATE_DOT', [ ('right1', port_type, port_depth), ('right2', port_type, port_depth) ] ] ] ]
#    input2 = { 'left': ['1', '2', '3'],
#               'right1': ['a', 'b', 'c'],
#               'right2': ['x', 'y', 'z'] }
#
#    # nested dot and cross (left (1,2,3,4,5,6,7,8,9) dot ( right1 (a,b,c) cross right2 (x,y,z) ) =>
#    #             1,2,3 . ( ax, ay, az, bx, by, bz, cx, cy, cz) =>
#    #             1ax, 2ay, 3az, 4bz, 5by, 6bz, 7cx, 8cy, 9cz
#    it3 = [ 'ITERATE_DOT', [ ('left', port_type, port_depth), [ 'ITERATE_CROSS', [ ('right1', port_type, port_depth), ('right2', port_type, port_depth) ] ] ] ]
#    input3 = { 'left': ['1', '2', '3', '4', '5', '6', '7', '8', '9'],
#               'right1': ['a', 'b', 'c'],
#               'right2': ['x', 'y', 'z'] }
#
#    # nested cross (left (1,2) dot ( right1 (a,b) cross right2 (x,y) ) =>
#    #                     1,2 . (ax, ay, bx, by) => 1ax, 1ay, 1by, 2ax, 2ay, 2bx, 2by
#
#
#    #print it3
#    #iterate_inputs(it3, input3)

    gwendia_file = '../examples/iter-test.gwendia'
    #gwendia_file = '../examples/dti_bedpost.gwendia'
    wx = WorkflowXML()
    wx.read_from_file(gwendia_file)
    #wx.text_out()

    build_tree(wx)
