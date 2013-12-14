#!/usr/bin/env python2.6

import time
import marvin

###############################################################################
#
# Main
#
if __name__ == '__main__':

    #gwendia_xml_file = '../examples/example.gwendia'
    gwendia_xml_file = 'dti_bedpost.gwendia'
    #gwendia_xml_file = 'iter-test.gwendia'

    awf = marvin.xml2abstract(gwendia_xml_file)
    awf.pg_draw()

    print 'EOF'
#
###############################################################################


