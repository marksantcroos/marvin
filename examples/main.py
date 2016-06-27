#!/usr/bin/env python2.6

import time
import marvin

import radical.pilot as rp


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

    pdesc = rp.ComputePilotDescription()
    pdesc.resource        = "local.localhost"
    pdesc.cores           = 1
    # pdesc.resource        = "osg.xsede-virt-clust"
    # pdesc.project         = 'TG-CCR140028'
    pdesc.runtime         = 60
    pdesc.cleanup         = False
    # pdesc.candidate_hosts = [
    #     #'MIT_CMS',
    #     #'UConn-OSG',
    #     '!SU-OG', # No compiler
    #     '!SU-OG-CE', #
    #     '!SU-OG-CE1', #
    #     '!CIT_CMS_T2', # Takes too long to bootstrap
    #     '!FIU_HPCOSG_CE', # zeromq build fails
    #     #'BU_ATLAS_Tier2',
    #     #'!UCSDT2', # Failing because of format character ...
    #     # '!MWT2', # No ssh
    #     '!SPRACE', # failing
    #     '!GridUNESP_CENTRAL', # On hold immediately.
    #     #'~(HAS_CVMFS_oasis_opensciencegrid_org =?= TRUE)'
    # ]

    pilot = pmgr.submit_pilots(pdesc)
    umgr.add_pilots(pilot)

    # report.info("Waiting for pilots to become active ...\n")
    pmgr.wait_pilots(pilot.uid , state=[rp.ACTIVE, rp.FAILED, rp.CANCELED])

    #gwendia_xml_file = '../examples/example.gwendia'
    #input_file = '../examples/gwendia_input_sample.xml'

    #gwendia_xml_file = 'dti_bedpost.gwendia'
    #input_file = 'dti_bedpost-input.xml'
    #inputfile = '../examples/dti7-noloop-input.xml'

    #gwendia_xml_file = 'iter-test.gwendia'

    gwendia_xml_file = 'bwa.gwendia'
    input_file = 'bwa_input.xml'

    ix = marvin.InputDataXML()
    ix.read_from_file(input_file)
    input = ix.list_inputs()
    print input

    awf = marvin.xml2abstract(gwendia_xml_file)
    awf.pg_draw('main.pdf')

    cwf = marvin.ConcreteWF()
    cwf.init(awf, input, umgr)

    time.sleep(120)

    cwf.deinit()

    session.close(cleanup=False)
    print 'EOF'
#
###############################################################################

