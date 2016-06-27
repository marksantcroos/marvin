#!/usr/bin/env python2.6

import time
from workflow_xml import WorkflowXML
from inputdata_xml import InputDataXML
from abstractwf import AbstractWF
from concretewf import ConcreteWF
from iteration import print_tree


###############################################################################
#
# Main
#
if __name__ == '__main__':

    # gwendia_file = '../examples/example.gwendia'
    gwendia_file = '../examples/dti_bedpost.gwendia'
    #gwendia_file = '../examples/iter-test.gwendia'

    # inputfile = '../examples/gwendia_input_sample.xml'
    #inputfile = '../examples/dti7-noloop-input.xml'
    inputfile = '../examples/dti_bedpost-input.xml'


    wx = WorkflowXML()
    wx.read_from_file(gwendia_file)
    #wx.text_out()
    wfe = wx.workflow

    ix = InputDataXML()
    ix.read_from_file(inputfile)
    data = ix.list_inputs()
    print data

    #print_tree(wx)

    awf = AbstractWF()
    awf.construct(wfe)
    awf.draw()

    cwf = ConcreteWF()
    cwf.init(awf, data)

    time.sleep(3)
    cwf.deinit()

    print 'EOF'
#
###############################################################################


