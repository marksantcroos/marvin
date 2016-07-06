#!/usr/bin/env python

import pykka
import time

import radical.utils as ru
report = ru.LogReporter(name='radical.pilot')

import radical.pilot as rp

import abstractwf
from gasw import gasw_repo

###############################################################################
#
# Source Actor
#
# Represents Input Nodes
# Will receive input values from Main process and will manage and forward the values to the connected nodes
#
class Source(pykka.ThreadingActor):

    ###########################################################################
    #
    def __init__(self, name):
        super(Source, self).__init__()
        self.name = name
        self.targets = []
        self.populated = False
        self._header = '[SOURCE: %s]' % self.name


    ###########################################################################
    #
    def on_start(self):
        report.info('%s Starting ...\n' % self._header)


    ###########################################################################
    #
    def on_stop(self):
        report.info('%s quitting ... \n' % self._header)

        for target in self.targets:
            report.info('%s Notifying to %s that Im done!\n' % (self._header, target.get_name().get()))
            target.notify_complete(self.name).get()

        report.plain('%s Done!\n' % (self._header))


    ###########################################################################
    #
    def link_to(self, target):
        report.info('%s Linking to %s\n' % (self._header, target.get_name().get()))
        self.targets.append(target)


    ###########################################################################
    #
    def fire(self):
        if not self.populated:
            raise Exception("%s Tried to link, but not yet populated!" % self._header)

        # TODO: take depth of target port into account?
        for idx, val in enumerate(self.value):
            for target in self.targets:
                report.info('%s Firing to %s\n' % (self._header, target.get_name().get()))
                target.add_input(self.name, idx, [val]).get()

        self.stop()


    ###########################################################################
    #
    def populate(self, value):
        report.warn('%s Populating with value: %s\n' % (self._header, value))
        self.value = value
        self.populated = True


###############################################################################
#
# Constant Actor
#
# Represents Constant input Nodes
# Will receive input values from Main process and will manage and forward the values to the connected nodes
#
class Constant(pykka.ThreadingActor):

    ###########################################################################
    #
    def __init__(self, name, value):
        super(Constant, self).__init__()
        self.name = name
        self.value = value
        self.targets = []
        self._header = '[CONST : %s]' % self.name


    ###########################################################################
    #
    def on_start(self):
        report.warn('%s Starting with value: %s... \n' % (self._header, self.value))


    ###########################################################################
    #
    def on_stop(self):
        report.info('%s quitting ... \n' % (self._header))

        for target in self.targets:
            report.info('%s Notifying to %s that Im done!\n' % (self._header, target.get_name().get()))
            target.notify_complete(self.name).get()

        report.info('%s Done!\n' % (self._header))


    ###########################################################################
    #
    def link_to(self, target):
        report.info('%s Linking to %s\n' % (self._header, target.get_name().get()))
        self.targets.append(target)


    ###########################################################################
    #
    def _send_input(self, target):
        report.warn('%s Sending input to %s\n' % (self._header, target.get_name().get()))
        target.add_input(self.name, 0, self.value).get()


    ###########################################################################
    #
    def fire(self):
        for target in self.targets:
            report.info('%s Firing to %s\n' % (self._header, target.get_name().get()))
            self._send_input(target)

        self.stop()


###############################################################################
#
# Sink Actor
#
class Sink(pykka.ThreadingActor):

    ###########################################################################
    #
    def __init__(self, name):
        super(Sink, self).__init__()
        self.name = name
        self.inputs = []
        self._header = '[SINK  : %s]' % self.name


    ###########################################################################
    #
    def on_start(self):
        report.info('%s Starting ... \n' % (self._header))


    ###########################################################################
    #
    def on_stop(self):
        report.info('%s quitting ... \n' % (self._header))
        report.warn('%s results received: %s\n' % (self._header, self.inputs))
        report.plain('%s Done!\n' % (self._header))


    ###########################################################################
    #
    def add_input(self, port, index, value):
        report.warn('%s Received input: %s on port: %s\n' % (self._header, value, port))
        self.inputs.append((port, value))


    ###########################################################################
    #
    def get_name(self):
        return self.name


    ###########################################################################
    #
    def notify_complete(self, port, index=-1):
        if index >= 0:
            report.info('%s Received completion notification from: %s for index: %d\n' % (self._header, port, index))
            return

        report.info('%s Received completion notification from: %s.\n' % (self._header, port))
        self.stop()


###############################################################################
#
# Port Actor
#
# TODO: Should ports just be an attribute of a processor?
#
class Port(pykka.ThreadingActor):

    ###########################################################################
    #
    def __init__(self, name, port_type, depth):
        super(Port, self).__init__()
        self.name = name
        self.port_type = port_type
        self.depth = depth
        self.targets = []
        self._header = '[PORT  : %s]' % self.name


    ###########################################################################
    #
    def on_start(self):
        report.info('%s Starting ... (type: %s, depth: %s)\n'
                % (self._header, self.port_type, self.depth))


    ###########################################################################
    #
    def on_stop(self):
        report.info('%s quitting ... \n' % (self._header))

        for target in self.targets:
            report.info('%s Sending completion notification to %s\n' % (self._header, target.get_name().get()))
            target.notify_complete(self.name.split(':')[1]).get()

        report.info('%s Done!\n' % (self._header))


    ###########################################################################
    #
    def link_to(self, target):
        report.info('%s Linking to %s\n' % (self._header, target.get_name().get()))
        self.targets.append(target)

        # If target is processor, create input ports
        try:
            target.add_input_port(self.name.split(':')[1], self.port_type, self.depth).get()
        except AttributeError as e:
            pass


    ###########################################################################
    #
    def add_input(self, port, index, value):
        report.warn('%s Received value: %s (index=%d)\n' % (self._header, value, index))
        self.value = value

        self._send_input(index, value)

        # if self.depth == 0:
        #     report.warn('%s depth=0, sending %s with index %d\n' % (self._header, value, index))
        #     self._send_input(index, value)
        # elif self.depth == 1:
        #     for i, v in enumerate(value):
        #         report.warn('%s depth=0, sending %s with index %d\n' % (self._header, value, index))
        #         self._send_input(i, v)
        # else:
        #     raise Exception('%s Depth > 1 not implemented.' % self._header)


    ###########################################################################
    #
    def notify_complete(self, port, index=-1):
        if index == -1:
            report.info('%s Received completion notification from: %s\n' % (self._header, port))
            self.stop()
        else:
            report.info('%s Received completion notification from: %s for index: %d\n' % (self._header, port, index))
            for target in self.targets:
                report.info('%s Sending completion notification to %s:%d\n' % (self._header, target.get_name().get(), index))
                target.notify_complete(self.name.split(':')[1], index).get()


    ###########################################################################
    #
    def _send_input(self, index, value):
        for target in self.targets:
            report.warn('%s Sending value[%d]: %s to %s\n' % (self._header, index, value, target.get_name().get()))
            target.add_input(self.name.split(':')[1], index, value).get()


    ###########################################################################
    #
    def get_name(self):
        return self.name


    ###########################################################################
    #
    def get_type(self):
        return self.port_type


    ###########################################################################
    #
    def get_depth(self):
        return self.depth


###############################################################################
#
# Processor Actor
#
class Processor(pykka.ThreadingActor):


    ###########################################################################
    #
    def _depth(self, l):
        if isinstance(l, list):
            return 1 + max(self._depth(item) for item in l)
        else:
            return 0


    ###########################################################################
    #
    def __init__(self, name, gasw, iter, umgr):
        super(Processor, self).__init__()
        self.name = name
        self._header = '[PROC  : %s]' % self.name
        self.gasw = gasw
        self.iter = iter
        self.targets = []
        self.umgr = umgr

        self.actor_refs = []
        # self.actor_proxies = {}

        self.task_no = 0
        self.inputs_complete = False

        self.running_tasks = {}

        self.input_ports = {}
        self.output_ports = {}
        self.inputs = {}
        # self.outputs = {}

        self.indices_created = []

    ###########################################################################
    #
    def on_start(self):
        report.info('%s Starting ...\n' % (self._header))


    ###########################################################################
    #
    def on_stop(self):

        report.info('%s quitting ...\n' % (self.name))

        for target in self.targets:
            report.info('%s Sending completion notification to %s\n' % (self._header, target.get_name().get()))
            target.notify_complete(self.name).get()

        report.info('%s Done!\n' % (self._header))

        # TODO: When to stop child tasks? (if at all)
        # for ref in self.actor_refs:
        #     ref.stop()


    ###########################################################################
    #
    def notify_complete(self, port, index=-1):
        if index >= 0:
            report.info('%s Received completion notification from: %s for index: %d\n' % (self._header, port, index))
            self.input_ports[port]['indices_complete'].append(index)

            if self.input_ports[port]['depth'] == 1:
                if index in self.indices_created:
                    report.info('%s Task already created at index: %d!\n' % (self._header, index))
                else:

                    report.info('%s Index complete and we are depth==1, fire of tasks!\n' % (self._header))
                    val = self.inputs[port][index]
                    self.indices_created.append(index)
                    self.create_task(index, val)

            return

        report.info('%s Received completion notification from: %s\n' % (self._header, port))


        self.input_ports[port]['complete'] = True
        report.info('%s after setting port complete\n' % (self._header))

        incomplete = 0
        for ip in self.input_ports:
            if self.input_ports[ip]['complete'] == False:
                incomplete += 1

        if incomplete:
            report.info("%s %d incomplete ports remaining.\n" % (self._header, incomplete))
        else:
            report.info("%s No incomplete ports remaining.\n" % (self._header))
            self.inputs_complete = True

        rt = 0
        for r in self.running_tasks:
            rt += self.running_tasks[r]
        report.warn("%s rt: %d\n" % (self._header, rt))
        if not rt and self.inputs_complete:
            report.info('%s Inputs complete and no running tasks, im done!\n' % (self._header))
            self.stop()


    ###########################################################################
    #
    def task_complete(self, task, index):
        report.info('%s Received task completion notification from: %s\n' % (self._header, task))
        self.running_tasks[index] -= 1
        report.warn('%s Tasks still running: %s; Inputs complete: %s\n' % (self._header, self.running_tasks, self.inputs_complete))

        # TODO: only have to check for index?
        rt = 0
        for r in self.running_tasks:
            rt += self.running_tasks[r]

            if not self.running_tasks[r]:
                report.warn('%s index: %d has no pending tasks\n' % (self._header, r))

                if self.inputs_complete:
                    report.warn('%s index:%d could notify completion!??!\n' % (self._header, r))
                    for target in self.targets:
                        report.info('%s Sending completion notification to %s for index: %d\n' % (self._header, target.get_name().get(), r))
                        target.notify_complete(self.name, index=r).get()
                else:
                    report.warn('%s index:%d could not notify completion!??!\n' % (self._header, r))

            else:
                report.warn('%s index: %d has pending tasks\n' % (self._header, r))

        report.warn("%s rt: %d\n" % (self._header, rt))
        if not rt and self.inputs_complete:
            report.info('%s Inputs complete and no running tasks, im done!\n' % (self._header))
            self.stop()


    ###########################################################################
    #
    def link_to(self, target):
        report.info('%s Linking to %s\n' % (self._header, target.get_name().get()))
        self.targets.append(target)
        self.add_output_port(target)


    ###########################################################################
    #
    def get_name(self):
        return self.name


    ###########################################################################
    #
    def add_input_port(self, name, port_type, depth):
        report.info('%s Add input port %s(%s)[%d]\n' % (self._header, name, port_type, depth))
        self.input_ports[name] = {'type': port_type, 'depth': depth, 'complete': False, 'indices_complete': []}
        self.inputs[name] = {}


    ###########################################################################
    #
    def add_output_port(self, proxy):
        name = proxy.get_name().get().split(':')[1],
        port_type = proxy.get_type().get()
        depth = proxy.get_depth().get()
        report.info('%s Add output port %s(%s)[%d]\n' % (self._header, name, port_type, depth))
        self.output_ports[name] = {'type': port_type, 'depth': depth, 'proxy': proxy}
        # self.outputs[name] = {}


    ###########################################################################
    #
    def add_input(self, port, index, value):
        report.warn('%s Received input[%d]: %s on port: %s\n' % (self._header, index, value, port))

        # Always starts at 1 level deep? Fix parser?
        if self.iter:
            iter = self.iter.child
            report.warn("%s Iteration strategy: %s\n" % (self._header, iter.strat))

        # TODO: Assuming depth of all input ports are the same for now
        input_port_depth = self.input_ports[port]['depth']

        # s2f / bwa / split
        if input_port_depth == 0:

            self.inputs[port][index] = value

            # for ip in self.input_ports:
            #     report.error("The depth of port %s is: %d\n" % (ip, self.input_ports[ip]['depth']))
            #     report.error("The depth of input on port %s is: %d\n" % (ip, len(self.inputs[ip])))
            #     try:
            #         report.error("The depth of input on port %s[%d] is: %d\n" % (ip, index, self._depth(self.inputs[ip][index])))
            #     except:
            #         report.warn("%s input port %s has no values at index %d\n" % (self._header, ip, index))

            for ip in self.input_ports:
                try:
                    if self.inputs[ip][index] == None:
                        report.warn("%s Has NULL value at index[%d]\n" % (self._header, index))
                        return
                except:
                    report.warn("%s Not all input ports have a value at index[%d]\n" % (self._header, index))
                    return

            report.warn("%s All input ports have a value at index[%d]\n" % (self._header, index))

            for val in self.inputs[port][index]:
                self.create_task(index, val)

        # merge
        elif input_port_depth == 1:

            try:
                self.inputs[port][index].append(value)
            except KeyError:
                self.inputs[port][index] = [value]

            # for ip in self.input_ports:
            #     report.error("The depth of port %s is: %d\n" % (ip, self.input_ports[ip]['depth']))
            #     report.error("The depth of input on port %s is: %d\n" % (ip, len(self.inputs[ip])))
            #     try:
            #         report.error("The depth of input on port %s[%d] is: %d\n" % (ip, index, self._depth(self.inputs[ip][index])))
            #     except:
            #         report.warn("%s input port %s has no values at index %d\n" % (self._header, ip, index))

            length = 2

            for ip in self.input_ports:
                try:
                    if self.inputs[ip][index] == None:
                        report.warn("%s Has NULL value at index[%d]\n" % (self._header, index))
                        return
                except:
                    report.warn("%s input port %s has no values at index %d\n" % (self._header, ip, index))
                    return

                report.warn("%s input port %s[%d] = %s\n" % (self._header, ip, index, self.inputs[ip][index]))
                #if len(self.inputs[ip][index]) < length:
                report.warn("%s input port %s[%d] indices_complete: %s\n" % (self._header, ip, index, self.input_ports[port]['indices_complete']))
                if index not in self.input_ports[port]['indices_complete']:
                    report.warn("%s %s[%d] does not have all input values (yet)\n" % (self._header, ip, index))
                    return

            report.warn("%s all input ports have all values at index %d\n" % (self._header, index))
            report.warn("%s skipping task creation\n" % (self._header))
            # val = self.inputs[port][index]
            # self.create_task(index, val)


    ###########################################################################
    #
    def create_task(self, index, input):


        name = '%s[%d]' % (self.name, self.task_no)
        # input = {ip: self.inputs[ip][index] for ip in self.inputs}
        ref = Task.start(self.actor_ref.proxy(), name, self.gasw, input, self.output_ports, index, self.task_no, self.umgr)
        self.actor_refs.append(ref)
        # self.actor_proxies[name] = ref.proxy()
        if index in self.running_tasks:
            self.running_tasks[index] += 1
        else:
            self.running_tasks[index] = 1
        self.task_no += 1


###############################################################################
#
# Task Actor
#
class Task(pykka.ThreadingActor):

    ###########################################################################
    #
    def __init__(self, processor, name, gasw, input, output_ports, index, task_no, umgr):
        super(Task, self).__init__()
        self.name = name
        self._header = '[TASK  : %s]' % self.name
        self.gasw = gasw
        self.input = input
        self.output_ports = output_ports
        self.umgr = umgr
        self.cu_id = None
        # self.cb_hist = {}
        self.processor = processor
        self.index = index
        self.task_no = task_no


    ###########################################################################
    #
    def on_start(self):
        report.info('%s Starting %s with %s ...\n' % (self._header, self.gasw, self.input))
        self.submit_cu()


    ###########################################################################
    #
    def submit_cu(self):
        gasw_desc = gasw_repo.get(self.gasw)

        cud = rp.ComputeUnitDescription()
        cud.executable = "/bin/bash"
        #cud.arguments = ["-c", "echo %s > STDOUT && %s %s" % (self.name, gasw_desc['executable'], " ".join(gasw_desc['arguments']))]
        cud.arguments = ["-c", "echo -n %s ... > STDOUT && %s %s && echo done >> STDOUT" % (self.name, gasw_desc['executable'], " ".join(gasw_desc['arguments']) if (self.task_no != 0 or self.gasw != 's2f.gasw') else "60")]
        cu = self.umgr.submit_units(cud)
        self.cu_id = cu.uid

        report.info('%s Launching %s (%s) %s...\n' % (self._header, gasw_desc['executable'], self.cu_id, self.input))

        # self.cb_hist[self.cu_id] = []
        cu.register_callback(self.cu_cb)


    ###########################################################################
    #
    def cu_cb(self, unit, state):
        # report.info("[Callback]: %s unit[%s]:'%s' on %s: %s.\n" % (self.actor_urn, unit.uid, self.name, unit.pilot_id, state))

        # print "[SchedulerCallback]: unit state callback history: %s" % (self.cb_hist)
        # if state in [rp.UNSCHEDULED] and rp.SCHEDULING in self.cb_hist[unit.uid]:
        #     print "[SchedulerCallback]: ComputeUnit %s with state %s already dealt with." % (unit.uid, state)
        #     return
        # self.cb_hist[unit.uid].append(state)

        if state == rp.DONE:
            if unit.uid != self.cu_id:
                report.error("Callback is not for me!!!!\n")

            self.cu_done()


    ###########################################################################
    #
    def cu_done(self):
        gasw_desc = gasw_repo.get(self.gasw)

        value = gasw_desc['function'](self.task_no, self.input)

        for op in self.output_ports:
            idx = self.index
            report.warn('%s Sending %s to %s[%d]\n' % (self._header, value, op, idx))
            self.output_ports[op]['proxy'].add_input(self.name, idx, value).get()

        try:
            report.info('%s calling self.stop()\n' % (self._header))
            self.stop()
        except Exception as e:
            report.error('%s not alive?!?! %s\n' % (self._header, e))


    ###########################################################################
    #
    def on_stop(self):
        report.info('%s Quitting ...\n' % (self._header))
        report.info('%s Notifying processor\n' % (self._header))
        self.processor.task_complete(self.name, self.index)
        report.info('%s Done!\n' % (self._header))


###############################################################################
#
class ConcreteWF(object):

    ###########################################################################
    #
    def __init__(self):
        pass

    ###########################################################################
    #
    # Create an abstract graph out of the internal workflow format
    # that was read from file
    #
    def init(self, awf, inputdata, umgr):
        report.info('Creating concrete workflow\n')

        self.awf = awf
        self.umgr = umgr

        self.actor_proxies = {}
        self.actor_refs = []

        for n in self.awf.list_input_nodes():

            if isinstance(n, abstractwf.Constant):

                report.info('Creating actor for Constant: %s\n' % n.name)

                ref = Constant.start(n.name, n.value)
                self.actor_refs.append(ref)
                self.actor_proxies[n.name] = ref.proxy()

            if isinstance(n, abstractwf.Source):
                report.info('Creating actor for Source: %s\n' % n.name)

                ref = Source.start(n.name)
                self.actor_refs.append(ref)
                proxy = ref.proxy()

                self.actor_proxies[n.name] = proxy

                if n.name in inputdata:
                    proxy.populate(inputdata[n.name])
                else:
                    raise Exception('Input not provided for port:' + n.name)

        for n in self.awf.list_proc_nodes():
            report.info('Creating actor for Processor: %s\n' % n.name)

            ref = Processor.start(n.name, n.gasw, n.iter, umgr)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()

        for n in self.awf.list_output_nodes():
            report.info('Creating actor for Sink: %s\n' % n.name)

            ref = Sink.start(n.name)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()

        for n in self.awf.list_port_nodes():
            report.info('Creating actor for Port: %s\n' % n.name)

            ref = Port.start(n.name, n.type, n.depth)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()

        for e in self.awf.list_edges(self.awf.graph.nodes()):
            # print 'Creating edges between Actors ...'
            # print e[0].name, e[1].name

            self.actor_proxies[e[0].name].link_to(self.actor_proxies[e[1].name]).get()

        for n in self.awf.list_input_nodes():
            self.actor_proxies[n.name].fire().get()


    ###########################################################################
    #
    def wait(self, timeout=None):
        refs = pykka.ActorRegistry.get_all()

        sleep = 1
        count = 0

        while True:
            alive = []
            for ref in refs:
                if ref.is_alive():
                    alive.append(ref)

            if not alive:
                report.info("No actors alive\n")
                break
            else:
                # report.info("Still %d actors alive\n" % len(alive))
                # print "Still %d actors alive (%s)" % (len(alive), [x.actor_class for x in alive])
                pass

            if timeout and count * sleep >= timeout:
                report.warn("Wait() timed out\n")
                break

            time.sleep(sleep)
            count += 1


    ###########################################################################
    #
    def deinit(self):
        report.info('Stopping all actors.\n')
        for ref in self.actor_refs:
            ref.stop()
        report.info('All actors stopped.\n')
