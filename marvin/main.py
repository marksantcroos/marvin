#!/usr/bin/env python2.6

import pykka
import time

###############################################################################
#
# PilotFlow: GWENDIA language based workflow engine for Pilot-API backends
#
#   Usage:
#           python pilotflow.py <workflow.xml> <input.xml>
#
#   workflow.xml: is a GWENDIA based workflow.
#   input.xml: the values to be used by the corresponding input ports.
#
#   Related Publications:
#
#   Pilot-API:
#       P*: A Model of Pilot-Abstractions.
#       Luckow, A., Santcroos, M., Merzky, A., Weidner, O., Mantha, P., & Jha, S. (2012).
#       Proceedings of the 8th IEEE International Conference on e-Science 2012.
#
#   GWENDIA:
#       A data-driven workflow language for grids based on array programming principles.
#       Montagnat, J., Isnard, B., Glatard, T., Maheshwari, K., & Fornarino, M. (2009).
#       WORKS '09: Proceedings of the 4th Workshop on Workflows in Support of Large-Scale Science.
#
###############################################################################


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

    def on_start(self):
        self.populated = False

    def link_to(self, target, port):
        if self.populated:
            print '[Source:%s] Linking to %s:%s\n' % (self.name, target.get_name().get(), port)
            target.add_input(port, self.value).get()
        else:
            raise Exception("Tried to link, but not yet populated!")

    def populate(self, value):
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

    def add_input(self, port, value):
        print '[Sink:%s] Received input: %s on port: %s' % (self.name, value, port)
        self.inputs.append((port, value))

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

    def on_start(self):
        self.input_ports = {}
        self.output_ports = {}
        self.inputs = {}

    def run(self):
        print '[Processor:%s] Running ...' % (self.name)

        if self.name == 'Square':
            print '[Processor:%s] running with value: %s' % (self.name, value)

            for (IGNORE, value) in self.inputs:
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

    def link_to(self, target, port):
        self.target = (target, port)

    def get_name(self):
        return self.name

    def add_input_port(self, name, type, depth):
        self.input_ports[name] = (type, depth)
        self.inputs[name] = []

    def add_output_port(self, name, type, depth):
        self.output_ports[name] = (type, depth)
        self.outputs[name] = []

    def add_input(self, port, value):
        print '[Processor:%s] Received input: %s on port: %s' % (self.name, value, port)
        self.inputs[port] = value



#
################################################################################


################################################################################
#
# Helper function to build iteration strategy
class IterationStrategy():
    def __init__(self):
        pass


#
################################################################################


###############################################################################
#
# Main
#
if __name__ == '__main__':





    src1_ref = Source.start('Input1')
    src1_proxy = src1_ref.proxy()

    src2_ref = Source.start('Input2')
    src2_proxy = src2_ref.proxy()

#    prc_ref= Processor.start('Square')
#    prc_proxy = prc_ref.proxy()

    prc_ref= Processor.start('Multiplier')
    prc_proxy = prc_ref.proxy()

    snk_ref = Sink.start('Result')
    snk_proxy = snk_ref.proxy()

    #src_proxy.populate(4).get()
    src1_proxy.populate(4).get()
    src2_proxy.populate(3).get()


    src1_proxy.link_to(prc_proxy, 'left').get()
    src2_proxy.link_to(prc_proxy, 'right').get()
    prc_proxy.link_to(snk_proxy, 80).get()
    prc_proxy.run().get()


#    src_proxy.link_to(snk_proxy, 80).get()



    src1_ref.stop()
    src2_ref.stop()
    prc_ref.stop()
    snk_ref.stop()
#
###############################################################################


