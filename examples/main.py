#!/usr/bin/env python2.6


import os
import sys
os.environ['RADICAL_SAGA_LOG_TGT'] = 'saga.log'
os.environ['RADICAL_PILOT_LOG_TGT'] = 'rp.log'
os.environ['RADICAL_PILOT_AGENT_VERBOSE'] = 'DEBUG'

import radical.pilot as rp

import logging
logging.basicConfig(filename='pykka.log', level=logging.DEBUG)

import marvin

LOCAL = "LOCAL"
OSG = "OSG"

CORES = 1

#------------------------------------------------------------------------------
#
CNT = 0
def unit_state_cb (unit, state):

    if not unit:
        return

    global CNT

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state in [rp.FAILED, rp.DONE, rp.CANCELED]:
        CNT += 1
        print "[Callback]: # %6d" % CNT

        # Hello HTC :-)
        #if state == rp.FAILED:
        #    print "stderr: %s" % unit.stderr
        #    sys.exit(2)

###############################################################################
#
# Main
#
if __name__ == '__main__':

    # we can optionally pass session name to RP
    if len(sys.argv) > 1:
        target = sys.argv[1]
    else:
        target = LOCAL

    print 'running on %s' % target

    session = rp.Session()
    print "session id: %s" % session.uid


    pmgr = rp.PilotManager(session=session)
    # pmgr.register_callback(pilot_state_cb)

    # SCHED = rp.SCHED_BACKFILLING
    SCHED = rp.SCHED_PILOTDATA
    # SCHED = rp.SCHED_DIRECT
    umgr = rp.UnitManager(session=session, scheduler=SCHED)
    # umgr.register_callback(unit_state_cb,      rp.UNIT_STATE)
    # umgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE)

    cpdesc = rp.ComputePilotDescription()
    cpdesc.cleanup = False
    cpdesc.runtime = 60

    if target == LOCAL:
        num_pilots = 1

        cpdesc.resource = "local.localhost"
        cpdesc.cores = CORES

    elif target == OSG:
        num_pilots = CORES

        cpdesc.cores = 1
        cpdesc.resource = "osg.xsede-virt-clust"
        cpdesc.project = 'TG-CCR140028'
        cpdesc.candidate_hosts = [
            #'MIT_CMS',
            #'UConn-OSG',
            '!SU-OG', # No compiler
            '!SU-OG-CE', #
            '!SU-OG-CE1', #
            '!CIT_CMS_T2', # Takes too long to bootstrap
            '!FIU_HPCOSG_CE', # zeromq build fails
            #'BU_ATLAS_Tier2',
            '!UCSDT2', # No cc
            # '!MWT2', # No ssh
            '!SPRACE', # failing
            '!GridUNESP_CENTRAL', # On hold immediately.
            #'~(HAS_CVMFS_oasis_opensciencegrid_org =?= TRUE)'
        ]
    else:
        raise Exception("Unknown target: %s" % target)

    # TODO: bulk submit pilots here
    pilots = []
    for p in range(num_pilots):
        pilot = pmgr.submit_pilots(cpdesc)
        umgr.add_pilots(pilot)
        pilots.append(pilot)

    dpds = []
    #for SE in [
    for SE in [
        #'MIT_CMS',
        #'LUCILLE',
        # Preferred SEs
        # 'cinvestav',
        # "osg.GLOW",
        # "SPRACE",
        # "SWT2_CPB", # DEAD
        # "osg.Nebraska",
        # "osg.UCSDT2",
        # "osg.UTA_SWT2",
        # "local.pd_tmp",
        "local.pd_home"
    ]:
        dpdesc = rp.DataPilotDescription()
        dpdesc.resource = '%s' % SE
        dpds.append(dpdesc)

    data_pilots = pmgr.submit_data_pilots(dpds)

    # if target == LOCAL:
    print("Waiting for pilots to become active ...\n")
    pmgr.wait_pilots([pilot.uid for pilot in pilots], state=[rp.ACTIVE, rp.FAILED, rp.CANCELED])

    #gwendia_xml_file = '../examples/example.gwendia'
    #input_file = '../examples/gwendia_input_sample.xml'

    #gwendia_xml_file = 'dti_bedpost.gwendia'
    #input_file = 'dti_bedpost-input.xml'
    #inputfile = '../examples/dti7-noloop-input.xml'

    #gwendia_xml_file = 'iter-test.gwendia'

    gwendia_xml_file = '../examples/bwa.gwendia'
    input_file = '../examples/bwa_input.xml'

    ix = marvin.InputDataXML()
    ix.read_from_file(input_file)
    input = ix.list_inputs()
    print input

    awf = marvin.xml2abstract(gwendia_xml_file)
    awf.pg_draw('main.pdf')

    cwf = marvin.ConcreteWF()
    cwf.init(awf, input, umgr, data_pilots)

    #time.sleep(120)

    cwf.wait(timeout=500)

    cwf.deinit()

    session.close(cleanup=False)

    print 'EOF'
#
###############################################################################
