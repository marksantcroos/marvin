#!/usr/bin/env python

import networkx as nx
import string

class Workflow(object):

    __slots__ = ( 'processors', 'name', 'sources', 'sinks', 'constants', 'links' )

    def __init__(self):
        self.name = None
        self.processors = []
        self.sources = []
        self.sinks = []
        self.constants = []
        self.links = []

    def text_out(self):

        def print_iter(p_iter, indent):

            if p_iter.i_type == 'ITERATION':
                print indent + 'Iteration:', p_iter.i_strat
            elif p_iter.i_type == 'PORT':
                print indent + 'Port:', p_iter.i_port

            if p_iter.i_next:
                print_iter(p_iter.i_next, indent)
            if p_iter.i_child:
                print_iter(p_iter.i_child, indent + '  ')

        print 'Workflow:', self.name
        print '  Interfaces:'
        for s in self.sources:
            print '    Source:', s.s_name, s.s_type
        for s in self.sinks:
            print '    Sink:', s.s_name, s.s_type
        for c in self.constants:
            print '    Constant:', c.c_name, c.c_type, c.c_value, c.c_card

        print '  Processors:'
        for p in self.processors:
            print '    Processor:', p.p_name

            print '      GASW:', p.p_gasw.g_desc
            for i in p.p_in:
                print '      Input:', i.p_name, i.p_type, i.p_depth
            for o in p.p_out:
                print '      Output:', o.p_name, o.p_type, o.p_depth

            if p.p_iter:
                print_iter(p.p_iter, '      ')

        print '  Links:'
        for l in self.links:
            print '    Link:', l.l_from, '->', l.l_to


class Node(object):
    pass

class Constant(Node):
    __slots__ = ( 'c_name', 'c_type', 'c_value', 'c_card' )

    def __init__(self):
        self.c_name = None
        self.c_type = None
        self.c_value = None
        self.c_card = None

class Sink(Node):
    __slots__ = ( 's_name', 's_type'  )

    def __init__(self):
        self.s_name = None
        self.s_type = None

class Source(Node):
    __slots__ = ( 's_name', 's_type'  )

    def __init__(self):
        self.s_name = None
        self.s_type = None

class Port(Node):

    __slots__ = ( 'p_name', 'p_type', 'p_depth' )

    def __init__(self):
        self.p_name = None
        self.p_type = None
        self.p_depth = None

class Processor(Node):

    __slots__ = ( 'p_in', 'p_out', 'p_gasw', 'p_name', 'p_iter' )

    def __init__(self):
        self.p_in = []
        self.p_out = []
        self.p_gasw = None
        self.p_name = None
        self.p_iter = None

class GASW(object):

    __slots__ = ( 'g_desc' )

    def __init__(self):
        self.g_desc= None

class Iteration(object):

    __slots__ = ( 'i_type', 'i_strat', 'i_port', 'i_parent', 'i_child', 'i_next', 'i_depth')

    def __init__(self):
        self.i_type = None # PORT, ITERATION
        self.i_strat = None # ITERATE_DOT, ITERATE_CROSS, ITERATE_FLAT_CROSS, ITERATE_MATCH
        self.i_port = None # Name of the port
        self.i_parent = None # Pointer to higher level
        self.i_child = None # pointer to deeper level
        self.i_next = None # point to next element on this level
        self.i_depth = None # Depth of iteration nesting


class Link(object):

    __slots__ = ( 'l_from', 'l_to' )

    def __init__(self):
        self.l_from = None
        self.l_to = None



class AbstractWF(object):

    def __init__(self):
        self.graph = nx.DiGraph()

    #
    # Create an abstract graph out of the internal workflow format 
    # that was read from file
    #
    def construct_from_xml(self, elements):
        """

        :param elements:
        """
        self.elements = elements

        self.nodes = dict()

        # Create Sink Nodes 
        for s in self.elements.sinks:
            node = Sink()
            node.name = s.s_name
            self.nodes[node.name] = node
            self.graph.add_node(node)

        # Create Processor Nodes 
        for p in self.elements.processors:
            node = Processor()
            node.name = p.p_name

            node.iter_strat = p.p_iter
            self.nodes[node.name] = node
            self.graph.add_node(node)

            node.in_ports = p.p_in
            for port in node.in_ports:
                pnode = Port()
                pnode.name = '%s:%s' % (node.name, port.p_name)
                pnode.depth = port.p_depth
                pnode.port_type = port.p_type

                self.nodes[pnode.name] = pnode
                self.graph.add_node(pnode)

                # Add connection from input port to node
                self.graph.add_edge(self.nodes[pnode.name], self.nodes[node.name])


            node.out_ports = p.p_out
            for port in node.out_ports:
                pnode = Port()
                pnode.name = '%s:%s' % (node.name, port.p_name)
                pnode.depth = port.p_depth
                pnode.port_type = port.p_type

                self.nodes[pnode.name] = pnode
                self.graph.add_node(pnode)

                # Add connection from output node to port
                self.graph.add_edge(self.nodes[node.name], self.nodes[pnode.name])


        # Create Source Nodes
        for s in self.elements.sources:
            node = Source()
            node.name = s.s_name
            self.nodes[node.name] = node
            self.graph.add_node(node)

        # Create Constant nodes
        for c in self.elements.constants:
            node = Constant()
            node.name = c.c_name
            node.value = c.c_value
            self.nodes[node.name] = node
            self.graph.add_node(node)

        # Iterate over links to create edges
        for l in self.elements.links:

            # Create the final edge between the nodes and/or ports
            self.graph.add_edge(self.nodes[l.l_from], self.nodes[l.l_to])
            
    #
    # Draw and display the graph
    #
    def draw(self):

        for n in self.graph.nodes():
            self.graph.node[n]['label'] = str(n.name)

            if isinstance(n, Processor):
                self.graph.node[n]['shape'] = 'doubleoctagon'

            elif isinstance(n, Source):
                self.graph.node[n]['shape'] = 'invtriangle'

            elif isinstance(n, Constant):
                self.graph.node[n]['shape'] = 'invhouse'

            elif isinstance(n, Sink):
                self.graph.node[n]['shape'] = 'triangle'

            elif isinstance(n, Port):
                self.graph.node[n]['shape'] = 'box'

        nx.write_dot(self.graph, 'awf.dot')

    #
    # Return the input nodes of the graph
    #
    def list_proc_nodes(self):

        result = []

        for n in self.graph.nodes():
            if isinstance(n, Processor):
                result.append(n)

        return result

    #
    # Return the port nodes of the graph
    #
    def list_port_nodes(self):

        result = []

        for n in self.graph.nodes():
            if isinstance(n, Port):
                result.append(n)

        return result
    #
    # Return the input nodes of the graph
    #
    def list_input_nodes(self):

        result = []

        for n in self.graph.nodes():
            count = len(self.graph.predecessors(n))

            if count == 0:
                result.append(n)

        return result

    #
    # Return the output nodes of the graph
    #
    def list_output_nodes(self):

        result = []

        for n in self.graph.nodes():
            count = len(self.graph.successors(n))
            if count == 0:
                result.append(n)

        return result

    def list_edges(self, nodes):
        return self.graph.edges(nodes)
