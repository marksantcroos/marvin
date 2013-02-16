#!/usr/bin/env python

import networkx as nx
import string

class Node(object):
    pass

class Processor(Node):
    pass

class Constant(Node):
    pass

class Sink(Node):
    pass

class Source(Node):
    pass

class Port(object):
    pass

class AbstractWF(object):

    def __init__(self):
        self.graph = nx.DiGraph()

    #
    # Create an abstract graph out of the internal workflow format 
    # that was read from file
    #
    def construct(self, elements):
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
            node.in_ports = p.p_in
            node.out_ports = p.p_out
            node.iter_strat = p.p_iter
            self.nodes[node.name] = node
            self.graph.add_node(node) 

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

            # Determine the src and dest nodes
            l_from_node = string.split(l.l_from, ':', 1)[0]
            l_to_node = string.split(l.l_to, ':', 1)[0]

            # Create the final edge between the nodes and/or ports
            self.graph.add_edge(self.nodes[l_from_node], \
                    self.nodes[l_to_node], dest=str(l.l_to))
            
            

    #
    # Draw and display the graph
    #
    def draw(self):

        for n in self.graph.nodes():
            self.graph.node[n]['label'] = str(n.name)

            if isinstance(n, Processor):
                self.graph.node[n]['shape'] = 'box'

            elif isinstance(n, Source):
                self.graph.node[n]['shape'] = 'triangle'

            elif isinstance(n, Constant):
                self.graph.node[n]['shape'] = 'triangle'

            elif isinstance(n, Sink):
                self.graph.node[n]['shape'] = 'diamond'

        nx.write_dot(self.graph, 'awf.dot')

    #
    # Return the input nodes of the graph
    #
    def proc_nodes(self):

        result = []

        for n in self.graph.nodes():
            if isinstance(n, Processor):
                result.append(n)

        return result

    #
    # Return the input nodes of the graph
    #
    def input_nodes(self):

        result = []

        for n in self.graph.nodes():
            count = len(self.graph.predecessors(n))
            if count == 0:
                result.append(n)

        return result

    #
    # Return the output nodes of the graph
    #
    def output_nodes(self):

        result = []

        for n in self.graph.nodes():
            count = len(self.graph.successors(n))
            if count == 0:
                result.append(n)

        return result


#
# Main
#
if __name__ == '__main__':

    from workflow_xml import WorkflowXML
    wx = WorkflowXML()
    wx.read_from_file('../examples/dti_bedpost.gwendia')
    #wx.text_out()
    wfe = wx.workflow

    awf = AbstractWF()
    awf.construct(wfe)
    awf.draw()



