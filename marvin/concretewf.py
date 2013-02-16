#!/usr/bin/env python

import string
from abstractwf import Constant, Source
from error import error
from execution import Instance, Executor

        
class ConcreteWF(object):

    def __init__(self):
        pass

    #
    # Create an abstract graph out of the internal workflow format 
    # that was read from file
    #
    def init(self, awf, inputdata):
        print 'Creating concrete workflow'

        self.awf = awf
        self.executor = Executor

        for n in self.awf.input_nodes():

            if isinstance(n, Constant):
                for f,t,d in self.awf.graph.out_edges(n, data=True):
                    print 'Assigning Constant "%s" ("%s") to "%s"' % \
                        (n.name, n.value, d['dest'])
                    d['values'] = [n.value]
                    d['hot'] = True


            if isinstance(n, Source):
                for f,t,d in self.awf.graph.out_edges(n, data=True):
                    print 'Assigning Source "%s" values to "%s"' \
                            % (n.name, d['dest'])
                    
                    for i in inputdata:
                        if i.s_name == n.name:
                            d['values'] = i.s_items

                    d['hot'] = True
              
    def set_hot_procs(self):

        print 'Now search for hot processors'
        for n in self.awf.proc_nodes():
            # Set all processors as hot XXX Why?
            n.hot = True
            for f,t,d in self.awf.graph.in_edges(n, data=True):
                if not ('hot' in d and d['hot'] is True):
                    n.hot = False

            print 'Processor "' + n.name + '" ready for execution:', n.hot

        

    #
    # Draw and display the graph
    #
    def instantiate(self):
        print 'Executing workflow'

        for n in self.awf.proc_nodes():
            if n.hot is True:
                print 'Going to execute:', n.name

                if not n.iter_strat:
                    error('No iteration strategy')
                else:
                    strat = n.iter_strat.i_strat
                    ports = n.iter_strat.i_ports

                if strat == 'dot':
                    pass

                elif strat == 'cross':
                    pass

                elif strat == 'flat-cross':
                    print 'Creating flat-cross processor'

                    port_inputs = []
                    
                    for p in ports:
                        print 'Ports:', p

                        for f,t,d in self.awf.graph.in_edges(n, data=True):
                            in_port = string.split(d['dest'], ':', 1)[1]

                            if in_port == p:
                                print 'found matching incoming port:', p
                                print 'length:', len(d['values'])
                                print d['values']
                                
                                port_inputs.append(d['values'])
                                
                    print 'port inputs:', port_inputs
                    
                    
                    cross = self.cross_product(port_inputs)

                    nr_instances = len(cross)
                    print 'Required concrete instances:', nr_instances
                    
                    for para in cross:
                        inst = Instance(para)
                        self.executor.add(inst)
                        
                elif strat == 'match':
                    pass

                else:
                    error('Unknown iteration strategy')
                    
                

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
                error('Oops, not all ports have the same length, not possible with dot product!')
                
        r = [[ports[i][j] for i in range(num_ports) ] for j in range(max_depth)]
                
        print 'Number of iterations', len(r)

        print r
    
        return r
    