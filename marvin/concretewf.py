#!/usr/bin/env python

import string
import abstractwf
import pykka



###############################################################################
#
# Source Actor
#
# Represents Input Nodes
# Will receive input values from Main process and will manage and forward the values to the connected nodes
#
class Source(pykka.ThreadingActor):
    def __init__(self, name):
        super(Source, self).__init__()
        self.name = name
        self.targets = []

    def on_start(self):
        self.populated = False
        print '[Source:%s] Starting ... ' % (self.name)

    def on_stop(self):
        print '[Source:%s] Done, quitting ... ' % (self.name)

    def link_to(self, target):
        print '[Source:%s] Linking to %s\n' % (self.name, target.get_name().get())
        self.targets.append(target)

    def fire(self):
        if not self.populated:
            raise Exception("Tried to link, but not yet populated!")

        for target in self.targets:
            print '[Source:%s] Firing to %s\n' % (self.name, target.get_name().get())
            target.add_input(self.value).get()

    def populate(self, value):
        print '[Source:%s] Populating with value: %s' % (self.name, value)
        self.value = value
        self.populated = True

#
###############################################################################

###############################################################################
#
# Constant Actor
#
# Represents Constant input Nodes
# Will receive input values from Main process and will manage and forward the values to the connected nodes
#
class Constant(pykka.ThreadingActor):
    def __init__(self, name, value):
        super(Constant, self).__init__()
        self.name = name
        self.value = value
        self.targets = []

    def on_start(self):
        print '[Constant:%s] Starting with value: %s... ' % (self.name, self.value)

    def on_stop(self):
        print '[Constant:%s] Done, quitting ... ' % (self.name)

    def link_to(self, target):
        print '[Constant:%s] Linking to %s\n' % (self.name, target.get_name().get())
        self.targets.append(target)

    def _send_input(self, target):
        print '[Constant:%s] Sending input to %s\n' % (self.name, target.get_name().get())
        target.add_input(self.value).get()

    def fire(self):
        for target in self.targets:
            print '[Constant:%s] Firing to %s\n' % (self.name, target.get_name().get())
            self._send_input(target)


#
###############################################################################


###############################################################################
#
# Sink Actor
#
class Sink(pykka.ThreadingActor):
    def __init__(self, name):
        super(Sink, self).__init__()
        self.name = name

    def on_start(self):
        self.inputs = []
        print '[Sink:%s] Starting ... ' % (self.name)

    def on_stop(self):
        print '[sink:%s] Done, quitting ... ' % (self.name)

    def add_input(self, port, value):
        print '[Sink:%s] Received input: %s on port: %s' % (self.name, value, port)
        self.inputs.append((port, value))

    def get_name(self):
        return self.name

#
###############################################################################


###############################################################################
#
# Port Actor
#
class Port(pykka.ThreadingActor):

    def _depth(self, l):
        if isinstance(l, list):
            return 1 + max(self._depth(item) for item in l)
        else:
            return 0

    def __init__(self, name, port_type, depth):
        super(Port, self).__init__()
        self.name = name
        self.port_type = port_type
        self.depth = depth
        self.targets = []

    def on_start(self):
        print '[Port:%s] Starting ... (type: %s, depth: %s)' \
                % (self.name, self.port_type, self.depth)

    def on_stop(self):
        print '[Port:%s] Done, quitting ... ' % (self.name)

    def link_to(self, target):
        print '[Port:%s] Linking to %s' % (self.name, target.get_name().get())
        self.targets.append(target)

    def add_input(self, value):
        print '[Port:%s] Received value: %s (depth=%s)' % (self.name, value, self._depth(value))
        self.value = value

        if self._depth(value) == 0:
            self._send_input(value)
        elif self._depth(value) == 1:
            for v in self.value:
                self._send_input(v)
        else:
            raise Exception('Higher depth not yet implemented')

    def _send_input(self, value):
        for target in self.targets:
            print '[Port:%s] Sending value: %s to %s' % (self.name, value, target)
            target.add_input(value).get()

    def get_name(self):
        return self.name

#
###############################################################################
###############################################################################
#
# Processor Actor
#
class Processor(pykka.ThreadingActor):
    def __init__(self, name):
        super(Processor, self).__init__()
        self.name = name
        self.targets = []


    def on_start(self):
        self.input_ports = {}
        self.output_ports = {}
        self.inputs = {}

    def run(self):
        print '[Processor:%s] Running ...' % (self.name)

        if self.name == 'Square':
            print '[Processor:%s] running' % (self.name)

            for (_, value) in self.inputs:
                input = value
                output = input * input
                print 'Im a squarer!\nInput: %s Output: %s' % (input, output)

        elif self.name == 'Multiplier':
            input1 = self.inputs['left']
            input2 = self.inputs['right']
            output = input1 * input2
            print 'Im a multiplier!\nInput1: %s Input2: %s Output: %s' % (input1, input2, output)


            target, port = self.target
            target.add_input(port, output).get()


    def link_to(self, target):
        print '[Processor:%s] Linking to %s' % (self.name, target.get_name().get())
        self.targets.append(target)

    def get_name(self):
        return self.name

    def add_input_port(self, name, proxy, type, depth):
        self.input_ports[name] = (proxy, type, depth)
        self.inputs[name] = []

    def add_output_port(self, name, type, depth):
        self.output_ports[name] = (type, depth)
        self.outputs[name] = []

    def add_input(self, value):
        print '[Processor:%s] Received input: %s on port: %s' % (self.name, value, 'unknown')
        #self.inputs[port] = value



#
################################################################################


################################################################################
#
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

        self.actor_proxies = {}
        self.actor_refs = []

        for n in self.awf.list_input_nodes():

            if isinstance(n, abstractwf.Constant):

                print 'Creating actor for Constant:', n.name

                ref = Constant.start(n.name, n.value)
                self.actor_refs.append(ref)
                self.actor_proxies[n.name] = ref.proxy()

                # for f,t,d in self.awf.graph.out_edges(n, data=True):
                #     print 'Assigning Constant "%s" ("%s") to "%s"' % \
                #         (n.name, n.value, d['dest'])
                #     d['values'] = [n.value]
                #     d['hot'] = True


            if isinstance(n, abstractwf.Source):
                print 'Creating actor for Source:', n.name

                ref = Source.start(n.name)
                self.actor_refs.append(ref)
                proxy = ref.proxy()

                self.actor_proxies[n.name] = proxy

                if n.name in inputdata:
                    proxy.populate(inputdata[n.name])
                else:
                    raise Exception('Input not provided for port:' + n.name)



                #
                # for f,t,d in self.awf.graph.out_edges(n, data=True):
                #     print 'Assigning Source "%s" values to "%s"' \
                #             % (n.name, d['dest'])
                    #
                    # for i in inputdata:
                    #     if i.s_name == n.name:
                    #         d['values'] = i.s_items
                    #
                    # d['hot'] = True
              
        for n in self.awf.list_proc_nodes():
            print 'Creating actor for Processor:', n.name

            ref = Processor.start(n.name)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()

            # Set all processors as hot XXX Why?
            # n.hot = True
            # for f,t,d in self.awf.graph.in_edges(n, data=True):
            #     if not ('hot' in d and d['hot'] is True):
            #         n.hot = False


        for n in self.awf.list_output_nodes():
            print 'Creating actor for Sink:', n.name

            ref = Sink.start(n.name)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()

        for n in self.awf.list_port_nodes():
            print 'Creating actor for Port:', n.name

            ref = Port.start(n.name, n.port_type, n.depth)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()

        for e in self.awf.list_edges(self.awf.graph.nodes()):
            print 'Creating edges between Actors ...'
            print e[0].name, e[1].name

            self.actor_proxies[e[0].name].link_to(self.actor_proxies[e[1].name]).get()

        for n in self.awf.list_input_nodes():
            self.actor_proxies[n.name].fire().get()





            #
    # Draw and display the graph
    #
    def instantiate(self):
        print 'Executing workflow'

        for n in self.awf.proc_nodes():
            if n.hot is True:
                print 'Going to execute:', n.name

                if not n.iter_strat:
                    raise('No iteration strategy')
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
                    raise('Unknown iteration strategy')


    def deinit(self):
        print 'Stopping all actors.'
        import time
        time.sleep(1)
        for ref in self.actor_refs:
            ref.stop()
        print 'All actors stopped.'


###############################################################################
#
# Main
#
if __name__ == '__main__':
    print 'hello'

#
#
#
#     src1_ref = Source.start('Input1')
#     src1_proxy = src1_ref.proxy()
#
#     src2_ref = Source.start('Input2')
#     src2_proxy = src2_ref.proxy()
#
# #    prc_ref= Processor.start('Square')
# #    prc_proxy = prc_ref.proxy()
#
#     prc_ref= Processor.start('Multiplier')
#     prc_proxy = prc_ref.proxy()
#
#     snk_ref = Sink.start('Result')
#     snk_proxy = snk_ref.proxy()
#
#     #src_proxy.populate(4).get()
#     src1_proxy.populate(4).get()
#     src2_proxy.populate(3).get()
#
#
#     src1_proxy.link_to(prc_proxy, 'left').get()
#     src2_proxy.link_to(prc_proxy, 'right').get()
#     prc_proxy.link_to(snk_proxy, 80).get()
#     prc_proxy.run().get()
#
#
# #    src_proxy.link_to(snk_proxy, 80).get()
#
#
#
#     src1_ref.stop()
#     src2_ref.stop()
#     prc_ref.stop()
#     snk_ref.stop()