#!/usr/bin/env python

import networkx as nx
import pygraphviz as pg
import string

class Node(object):
    pass

class Constant(Node):
    __slots__ = ('name', 'type', 'value', 'card')

    def __init__(self):
        self.name = None
        self.type = None
        self.value = None
        self.card = None

class Sink(Node):
    __slots__ = ('name', 'type')

    def __init__(self):
        self.name = None
        self.type = None

class Source(Node):
    __slots__ = ('name', 'type')

    def __init__(self):
        self.name = None
        self.type = None

class Port(Node):

    __slots__ = ('name', 'type', 'depth')

    def __init__(self):
        self.name = None
        self.type = None
        self.depth = None

class Processor(Node):

    __slots__ = ('in', 'out', 'gasw', 'name', 'iter')

    def __init__(self):
        self.name = None
        self.input = []
        self.output = []
        self.gasw = None
        self.iter = None

# class GASW(object):
#
#     __slots__ = ('desc')
#
#     def __init__(self):
#         self.desc = None

class Iteration(object):

    __slots__ = ('type', 'strat', 'port', 'parent', 'child', 'next', 'depth')

    def __init__(self):
        self.type = None # PORT, ITERATION
        self.strat = None # ITERATE_DOT, ITERATE_CROSS, ITERATE_FLAT_CROSS, ITERATE_MATCH
        self.port = None # Name of the port
        self.parent = None # Pointer to higher level
        self.child = None # pointer to deeper level
        self.next = None # point to next element on this level
        self.depth = None # Depth of iteration nesting


class Link(object):

    __slots__ = ('tail', 'head')

    def __init__(self):
        self.tail = None
        self.head = None


class AbstractWF(object):

    def __init__(self):
        self.graph = nx.DiGraph()
        self.nodes = dict()

    def add_node(self, name, node):
        #print '### Add node: %s' % name
        self.nodes[name] = node
        self.graph.add_node(node)

    def add_edge(self, tail_name, head_name):
        #print '### Add edge: %s -> %s' % (tail_name, head_name)
        self.graph.add_edge(self.nodes[tail_name], self.nodes[head_name])

    # Check if a node with this name already exists
    def exist_node(self, name):
        return name in self.nodes


    #
    # Draw and display the graph
    #
    def nx_draw(self):

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
                self.graph.node[n]['label'] = n.label

        nx.write_dot(self.graph, 'awf.dot')

    #
    # Draw and display the graph with pygraphviz
    #
    def pg_draw(self):

        graph = pg.AGraph()

        for nx_node in self.graph.nodes():

            style='solid'
            fillcolor=''
            color='black'

            if isinstance(nx_node, Processor):
                shape = 'octagon'
                style='filled'
                fillcolor='white'

            elif isinstance(nx_node, Source):
                #shape = 'invtriangle'
                shape = 'invhouse'

            elif isinstance(nx_node, Constant):
                shape = 'invtriangle'
                #shape = 'invhouse'

            elif isinstance(nx_node, Sink):
                #shape = 'triangle'
                shape = 'diamond'

            elif isinstance(nx_node, Port):
                shape = 'box'
                style='filled'
                fillcolor='white'

            if hasattr(nx_node, 'label'):
                label = nx_node.label
            else:
                label = nx_node.name
            graph.add_node(str(nx_node.name), shape=shape, style=style, color=color, fillcolor=fillcolor, label=label)

        for nx_node in self.graph.nodes():
            # Find all the processors,
            if isinstance(nx_node, Processor):

                # All neighbors of the Processor (a.k.a.: just ports)
                nb = [node.name for node in nx.all_neighbors(self.graph, nx_node)]

                # Group processors and ports into subgraphs
                graph.subgraph(nb+[nx_node.name], name='cluster:'+nx_node.name,
                        style='filled', fillcolor='lightgrey')#,


        # Draw all the edges
        for f,t in self.graph.edges():
            graph.add_edge(f.name, t.name, dir='forward')
            #if not (isinstance(f, Processor) or isinstance(t, Processor)):
            #    graph.add_edge(f.name, t.name, dir='forward')
            #else:
            #    graph.add_edge(f.name, t.name, style='invisible')


        # Create legend
        graph.add_node('Output', shape='diamond', style='filled', fillcolor='white')
        graph.add_node('Compute Unit', shape='octagon', style='filled', fillcolor='white')
        graph.add_node('Port', shape='box', style='filled', fillcolor='white')
        graph.add_node('Input', shape='invhouse', style='filled', fillcolor='white')
        graph.subgraph(['Output', 'Port', 'Compute Unit', 'Input'], label='Legend', name='cluster_legend', style='filled', fillcolor='lightgrey')

        # Output graph to file
        #graph.write('awf.dot')
        graph.draw('awf.pdf', prog='dot', format='pdf')


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
