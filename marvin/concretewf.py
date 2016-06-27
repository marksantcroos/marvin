#!/usr/bin/env python

import abstractwf
import pykka

import time

from gasw import gasw_repo

import radical.pilot as rp
import threading

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
            target.add_input(self.name, 0, self.value).get()

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
        target.add_input(self.name, 0, self.value).get()

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

    def add_input(self, port, index, value):
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

        # If target is processor, create input ports
        try:
            target.add_input_port(self.name.split(':')[1], self.port_type, self.depth).get()
        except AttributeError as e:
            pass


    def add_input(self, port, index, value):
        print '[Port:%s] Received value: %s (depth=%d)' % (self.name, value, self._depth(value))
        self.value = value

        if self._depth(value) == 0:
            self._send_input(0, value)
        elif self._depth(value) == 1:
            for i, v in enumerate(value):
                self._send_input(i, v)
        else:
            raise Exception('Depth > 1 not implemented.')


    def _send_input(self, index, value):
        for target in self.targets:
            print '[Port:%s] Sending value[%d]: %s to %s' % (self.name, index, value, target.get_name().get())
            target.add_input(self.name.split(':')[1], index, value).get()


    def get_name(self):
        return self.name

    def get_type(self):
        return self.port_type

    def get_depth(self):
        return self.depth


#
###############################################################################

###############################################################################
#
# Processor Actor
#
class Processor(pykka.ThreadingActor):
    def __init__(self, name, gasw, iter, umgr):
        super(Processor, self).__init__()
        self.name = name
        self.gasw = gasw
        self.iter = iter
        self.targets = []
        self.umgr = umgr

        self.actor_refs = []
        self.actor_proxies = {}

        self.task_index = 0


    def on_start(self):
        print '[Processor:%s] Starting ... ' % (self.name)
        self.input_ports = {}
        self.output_ports = {}
        self.inputs = {}
        # self.outputs = {}


    def on_stop(self):
        print '[Processor:%s] Done, quitting ... ' % (self.name)

        for ref in self.actor_refs:
            ref.stop()



    def link_to(self, target):
        print '[Processor:%s] Linking to %s' % (self.name, target.get_name().get())
        self.targets.append(target)
        self.add_output_port(target)


    def get_name(self):
        return self.name


    def add_input_port(self, name, port_type, depth):
        print '[Processor:%s] Add input port %s(%s)[%d]' % (self.name, name, port_type, depth)
        self.input_ports[name] = {'type': port_type, 'depth': depth}
        self.inputs[name] = {}


    def add_output_port(self, proxy):
        name = proxy.get_name().get().split(':')[1],
        port_type = proxy.get_type().get()
        depth = proxy.get_depth().get()
        print '[Processor:%s] Add output port %s(%s)[%d]' % (self.name, name, port_type, depth)
        self.output_ports[name] = {'type': port_type, 'depth': depth, 'proxy': proxy}
        # self.outputs[name] = {}


    def add_input(self, port, index, value):
        print '[Processor:%s] Received input[%d]: %s on port: %s' % (self.name, index, value, port)
        self.inputs[port][index] = value

        for ip in self.input_ports:
            try:
                if self.inputs[ip][index] == None:
                    print "[Processor:%s] Has NULL value at index[%d]" % (self.name, index)
                    return
            except:
                print "[Processor:%s] Not all input ports have a value at index[%d]" % (self.name, index)
                return

        print "[Processor:%s] All input ports have a value at index[%d]" % (self.name, index)

        self.create_task(index)


    def create_task(self, index):
        name = '%s[%d]' % (self.name, self.task_index)
        self.task_index += 1
        input = {ip: self.inputs[ip][index] for ip in self.inputs}
        ref = Task.start(name, self.gasw, input, self.output_ports, self.umgr)
        self.actor_refs.append(ref)
        self.actor_proxies[name] = ref.proxy()

#
################################################################################



###############################################################################
#
# Task Actor
#
class Task(pykka.ThreadingActor):
    def __init__(self, name, gasw, input, output_ports, umgr):
        super(Task, self).__init__()
        self.name = name
        self.gasw = gasw
        self.input = input
        self.output_ports = output_ports
        self.umgr = umgr
        self.cu_id = None
        self.lock = threading.RLock ()

    def on_start(self):
        print '[Task:%s] Starting %s with %s ...' % (self.name, self.gasw, self.input)

        self.submit_cu()


    def submit_cu(self):
        gasw_desc = gasw_repo.get(self.gasw)

        cud = rp.ComputeUnitDescription()
        # cud.executable = "/bin/bash"
        # cud.arguments = ["-c", "echo %s && sleep %s" % (self.name, 0)]
        cud.executable = gasw_desc['executable']
        cud.arguments = gasw_desc['arguments']
        cu = self.umgr.submit_units(cud)
        self.cu_id = cu.uid

        print '[Task:%s] Launching %s (%s) %s...' % (self.name, gasw_desc['executable'], self.cu_id, self.input)

        cu.register_callback(self.cu_cb)


    def cu_cb(self, unit, state):
        print "[Callback]: unit[%s]:'%s' on %s: %s." % (unit.uid, self.name, unit.pilot_id, state)

        if state == rp.DONE:
            if unit.uid != self.cu_id:
                print "ERROR: Callback is not for me!!!!"

            self.cu_done()

    def cu_done(self):

        print '[Task:%s] Sending output to %s' % (self.name, self.output_ports.keys())
        for op in self.output_ports:
            depth = self.output_ports[op]['proxy'].get_depth().get()
            print '[Task:%s] output port has depth: %d' % (self.name, depth)

            if depth == 0:
                value = 42
            elif depth == 1:
                chunks = int(self.input['in_chunks'])
                value = [42] * chunks

            self.output_ports[op]['proxy'].add_input(self.name, depth, value).get()

        self.stop()

    # def run(self):
    #     print '[Processor:%s] Running ...' % (self.name)
    #
    #     if self.name == 'Square':
    #         print '[Processor:%s] running' % (self.name)
    #
    #         for (_, value) in self.inputs:
    #             input = value
    #             output = input * input
    #             print 'Im a squarer!\nInput: %s Output: %s' % (input, output)
    #
    #     elif self.name == 'Multiplier':
    #         input1 = self.inputs['left']
    #         input2 = self.inputs['right']
    #         output = input1 * input2
    #         print 'Im a multiplier!\nInput1: %s Input2: %s Output: %s' % (input1, input2, output)
    #
    #         target, port = self.target
    #         target.add_input(port, output).get()

    def on_stop(self):
        print '[Task:%s] Done, quitting ... ' % (self.name)


################################################################################
#
class ConcreteWF(object):

    def __init__(self):
        pass

    #
    # Create an abstract graph out of the internal workflow format 
    # that was read from file
    #
    def init(self, awf, inputdata, umgr):
        print 'Creating concrete workflow'

        self.awf = awf
        self.umgr = umgr

        self.actor_proxies = {}
        self.actor_refs = []

        for n in self.awf.list_input_nodes():

            if isinstance(n, abstractwf.Constant):

                print 'Creating actor for Constant:', n.name

                ref = Constant.start(n.name, n.value)
                self.actor_refs.append(ref)
                self.actor_proxies[n.name] = ref.proxy()


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


        for n in self.awf.list_proc_nodes():
            print 'Creating actor for Processor:', n.name

            ref = Processor.start(n.name, n.gasw, n.iter, umgr)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()


        for n in self.awf.list_output_nodes():
            print 'Creating actor for Sink:', n.name

            ref = Sink.start(n.name)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()


        for n in self.awf.list_port_nodes():
            print 'Creating actor for Port:', n.name

            ref = Port.start(n.name, n.type, n.depth)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()


        for e in self.awf.list_edges(self.awf.graph.nodes()):
            # print 'Creating edges between Actors ...'
            # print e[0].name, e[1].name

            self.actor_proxies[e[0].name].link_to(self.actor_proxies[e[1].name]).get()


        for n in self.awf.list_input_nodes():
            self.actor_proxies[n.name].fire().get()


    def deinit(self):
        print 'Stopping all actors.'
        # import time
        # time.sleep(1)
        for ref in self.actor_refs:
            ref.stop()
        print 'All actors stopped.'
