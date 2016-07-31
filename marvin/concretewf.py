#!/usr/bin/env python

import pykka
import time
import os

from string import Template

import radical.utils as ru
report = ru.LogReporter(name='radical.pilot')

import radical.pilot as rp

import abstractwf
from gasw import gasw_repo

import itertools

def _flatten(l):
    return _flatten(l[0]) + (
        _flatten(l[1:]) if len(l) > 1 else []) if type(l) is list else [l]


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
    def __init__(self, name, umgr, data_pilots, du_selection):
        super(Source, self).__init__()
        self.name = name
        self.targets = []
        self.populated = False
        self._header = '[SOURCE: %s]' % self.name
        self.umgr = umgr
        self.data_pilots = data_pilots
        self.du_selection = du_selection


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
    # Called by the API once all elements are created and inputs are provided.
    #
    def fire(self):
        if not self.populated:
            raise Exception("%s Tried to link, but not yet populated!" % self._header)

        # TODO: take depth of target port into account?
        for idx, val in enumerate(self.data_units):
            for target in self.targets:
                report.info('%s Firing to %s\n' % (self._header, target.get_name().get()))
                target.add_input(self.name, idx, [val]).get()

        self.stop()


    ###########################################################################
    #
    # This add input values to, which is added through the API when parsing input files
    #
    def populate(self, value):
        report.warn('%s Populating with value: %s\n' % (self._header, value))
        # self.value = value
        self.populated = True

        self.data_units = []
        for val in value:

            dud = rp.DataUnitDescription()
            dud.name = val
            dud.files = [val]
            dud.size = 1
            dud.selection = self.du_selection

            du = self.umgr.submit_data_units(dud, data_pilots=self.data_pilots, existing=True)
            print "data unit %s with %s available on data pilots: %s" % (du.uid, val, du.pilot_ids)

            self.data_units.append(du)


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
    # Called by the API once all elements are created and inputs are provided.
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
    # Sink gets values submitted by its input port when ...
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
    # Sink gets notified by its input port when ...
    #
    def notify_complete(self, port, index=-1):
        if index >= 0:
            report.info('%s Received completion notification from: %s for index: %d\n' % (self._header, port, index))

            # If this is a completion for an index only, then don't stop yet
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
    # Notified when output port of input component is done.
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
    def __init__(self, name, gasw, iter, umgr, data_pilots, du_selection):
        super(Processor, self).__init__()
        self.name = name
        self._header = '[PROC  : %s]' % self.name
        self.gasw = gasw
        self.iter = iter
        self.targets = []
        self.umgr = umgr
        self.data_pilots = data_pilots
        self.du_selection = du_selection

        self.actor_refs = []
        # self.actor_proxies = {}

        self.task_no = 0
        self.inputs_complete = False

        self.running_tasks = {}

        self.input_ports = {}
        self.output_ports = {}
        self.inputs = {}

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
    # Called by input port when XXX, index is set when XXX
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
                    self.create_task(index, [val])

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
        report.warn("%s Running tasks: %d\n" % (self._header, rt))
        if not rt and self.inputs_complete:
            report.info('%s Inputs complete and no running tasks, im done!\n' % (self._header))
            self.stop()


    ###########################################################################
    #
    # This method is called from Task:on_stop()
    #
    def task_complete(self, task, index):

        report.info('%s Received task completion notification from: %s\n' % (self._header, task))
        self.running_tasks[index] -= 1
        report.warn('%s Other tasks still running: %s; Inputs complete: %s\n' % (self._header, self.running_tasks, self.inputs_complete))

        if self.running_tasks[index] == 0:
            report.warn('%s index: %d has no pending tasks\n' % (self._header, index))

            all_indices_complete = True
            for port in self.input_ports:
                report.warn("%s port: %s:%d -- %s\n" % (self._header, port, index, self.input_ports[port]['indices_complete']))
                if index not in self.input_ports[port]['indices_complete']:
                    all_indices_complete = False
                    break

            if all_indices_complete or self.inputs_complete:
                report.warn('%s index:%d could notify completion!??!\n' % (self._header, index))
                for target in self.targets:
                    report.info('%s Sending completion notification to %s for index: %d\n' % (self._header, target.get_name().get(), index))
                    target.notify_complete(self.name, index=index).get()
            else:
                report.warn('%s index:%d could not notify completion!??!\n' % (self._header, index))

        else:
            report.warn('%s index: %d has pending tasks\n' % (self._header, index))

        num_running = 0
        for task_index in self.running_tasks:
            num_running += self.running_tasks[task_index]
        report.warn("%s Running tasks: %d\n" % (self._header, num_running))
        if num_running == 0 and self.inputs_complete:
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

                    # if ip == 'in_ReferenceTarGz':
                    #     report.warn("%s Special case for in_ReferenceTarGz\n" % (self._header))
                    #     continue

                    return

            report.warn("%s All input ports have a value at index[%d]\n" % (self._header, index))

            for ip in self.inputs:
                report.error("%s input port %s[%d]: %s\n" % (self._header, ip, index, self.inputs[ip][index]))

            inputs = [list(x) for x in itertools.product(*[_flatten(self.inputs[ip][index]) for ip in self.input_ports])]

            report.error("%s inputs after mangling: %s\n" % (self._header, inputs))

            # inputs = self.inputs[port][index]
            for val in inputs:
                report.warn("%s Creating task with '%s' as input(s)" % (self._header, val))
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


    ###########################################################################
    #
    def create_task(self, index, input):

        name = '%s[%d]' % (self.name, self.task_no)

        ref = Task.start(self.actor_ref.proxy(), name, self.gasw, input, self.output_ports, index, self.task_no, self.umgr, self.data_pilots, self.du_selection)
        self.actor_refs.append(ref)

        # Record submitted tasks for this index
        if index in self.running_tasks:
            self.running_tasks[index] += 1
        else:
            self.running_tasks[index] = 1

        # Maintain task instance id
        self.task_no += 1


###############################################################################
#
# Task Actor
#
class Task(pykka.ThreadingActor):

    ###########################################################################
    #
    def __init__(self, processor, name, gasw, input, output_ports, index, task_no, umgr, data_pilots, du_selection):
        super(Task, self).__init__()
        self.name = name
        self._header = '[TASK  : %s]' % self.name
        self.gasw = gasw
        self.input = input
        self.output_ports = output_ports
        self.umgr = umgr
        self.data_pilots = data_pilots
        self.cu_id = None
        self.processor = processor
        self.index = index
        self.task_no = task_no
        self.du_selection = du_selection


    ###########################################################################
    #
    def on_start(self):
        report.info('%s Starting %s with %s ...\n' % (self._header, self.gasw, self.input))
        self.submit_cu()


    ###########################################################################
    #
    def submit_cu(self):

        gasw_desc = gasw_repo.get(self.gasw)

        report.warn("%s self.input from submit: %s" % (self._header, self.input))

        self.input = _flatten(self.input)
        report.warn("%s self.input after flatten: %s" % (self._header, self.input))

        input_label = None
        for du in self.input:
            input_label = du.description.files[0]
            input_label = os.path.basename(input_label)
            # TODO: implement proper expansion of filenames based on port names
            if input_label not in [
                'reference_1M', 'reference_10M', 'reference_100M', 'reference_1000M',
                '1M', '10M', '100M', '1000M', '10000M'
            ]:
                break

        report.warn("%s input label: %s" % (self._header, input_label))

        output = []
        for e in gasw_desc['output']:
            if input_label:
                t = Template(e)
                e = t.safe_substitute({'INPUT': input_label})

            output.append(e)

        # TODO: Create output DU per port
        dud = rp.DataUnitDescription()
        # dud.name = 'output'
        dud.files = output
        dud.size = 1
        dud.selection = self.du_selection

        du = self.umgr.submit_data_units(dud, data_pilots=self.data_pilots, existing=False)
        print "data unit: %s will be available on data pilots: %s" % (du.uid, du.pilot_ids)

        # Record the DU as the output for this task
        self.output = [du]

        # Construct CU
        cud = rp.ComputeUnitDescription()
        if 'pre_exec' in gasw_desc:
            cud.pre_exec = gasw_desc['pre_exec']
        cud.executable = gasw_desc['executable']
        cud.arguments = []
        for arg in gasw_desc['arguments']:
            if input_label:
                t = Template(arg)
                arg = t.safe_substitute({'INPUT': input_label})

            cud.arguments.append(arg)

        cud.input_data = [d.uid for d in self.input]

        cud.output_data = [du.uid]

        # Submit the unit
        cu = self.umgr.submit_units(cud)

        # Couple the CU ID to this task
        self.cu_id = cu.uid

        # Register CU for callbacks
        cu.register_callback(self.cu_cb)

        report.info('%s Launching %s (args:%s) (id:%s) (input:%s)...\n' % (self._header, cud.executable, cud.arguments, self.cu_id, cud.input_data))


    ###########################################################################
    #
    def cu_cb(self, unit, state):
        # report.info("[Callback]: %s unit[%s]:'%s' on %s: %s.\n" % (self.actor_urn, unit.uid, self.name, unit.pilot_id, state))

        if state == rp.DONE:
            if unit.uid != self.cu_id:
                report.error("Callback is not for me!!!!\n")

            self.cu_done()

        # TODO: what else to do for failed units?
        elif state == rp.FAILED:
            report.error('%s unit %s failed\n' % (self._header, unit.uid))
            self.stop()


    ###########################################################################
    #
    def cu_done(self):
        gasw_desc = gasw_repo.get(self.gasw)

        if 'post_function' in gasw_desc:
            gasw_desc['post_function'](self)

        for op in self.output_ports:
            idx = self.index
            report.warn('%s Sending %s to %s[%d]\n' % (self._header, self.output, op, idx))
            self.output_ports[op]['proxy'].add_input(self.name, idx, self.output).get()

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
    def init(self, awf, inputdata, umgr, data_pilots, du_selection):
        report.info('Creating concrete workflow\n')

        self.actor_proxies = {}
        self.actor_refs = []

        for n in awf.list_input_nodes():

            if isinstance(n, abstractwf.Constant):

                report.info('Creating actor for Constant: %s\n' % n.name)

                ref = Constant.start(n.name, n.value)
                self.actor_refs.append(ref)
                self.actor_proxies[n.name] = ref.proxy()

            if isinstance(n, abstractwf.Source):
                report.info('Creating actor for Source: %s\n' % n.name)

                ref = Source.start(n.name, umgr, data_pilots, du_selection)
                self.actor_refs.append(ref)
                proxy = ref.proxy()

                self.actor_proxies[n.name] = proxy

                if n.name in inputdata:
                    proxy.populate(inputdata[n.name])
                else:
                    raise Exception('Input not provided for port:' + n.name)

        for n in awf.list_proc_nodes():
            report.info('Creating actor for Processor: %s\n' % n.name)

            ref = Processor.start(n.name, n.gasw, n.iter, umgr, data_pilots, du_selection)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()

        for n in awf.list_output_nodes():
            report.info('Creating actor for Sink: %s\n' % n.name)

            ref = Sink.start(n.name)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()

        for n in awf.list_port_nodes():
            report.info('Creating actor for Port: %s\n' % n.name)

            ref = Port.start(n.name, n.type, n.depth)
            self.actor_refs.append(ref)
            self.actor_proxies[n.name] = ref.proxy()

        for e in awf.list_edges(awf.graph.nodes()):
            # print 'Creating edges between Actors ...'
            # print e[0].name, e[1].name

            self.actor_proxies[e[0].name].link_to(self.actor_proxies[e[1].name]).get()

        for n in awf.list_input_nodes():
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
