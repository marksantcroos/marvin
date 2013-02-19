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


#
# Main
#
if __name__ == '__main__':

    from workflow_xml import WorkflowXML
    wx = WorkflowXML()
    wx.read_from_file('../examples/dti_bedpost.gwendia')
    wx.text_out()
    wfe = wx.workflow

    awf = AbstractWF()
    awf.construct(wfe)
    awf.draw()






