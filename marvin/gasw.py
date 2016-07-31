import copy
import string
import radical.pilot as rp
import math

import radical.utils as ru
report = ru.LogReporter(name='radical.pilot')

###########################################################################
#
def s2f(task_no, input):
    return ['fastq%d_%s' % (task_no, input)]


###########################################################################
#
def split(self):
    orig = self.output[0]
    self.output = []
    print 'files: %s' % orig.description.files
    for file in orig.description.files:
        dud = rp.DataUnitDescription()
        dud.name = file
        # TODO: can the path be derived from the original?
        dud.files = ['tmp/%s/%s/%s' % (self.umgr._session.uid, orig.uid, file)]
        dud.size = orig.description.size / len(orig.description.files)
        report.error("setting splitted du size to: %d" % dud.size)
        dud.selection = self.du_selection

        # TODO: grab the DP from the orig DU and use it for the new DU

        du = self.umgr.submit_data_units(dud, data_pilots=orig.pilot_ids, existing=True)

        # TODO: Move into new directory?

        self.output.append(du)


###########################################################################
#
def bwa(task_no, input):
    return ['%s.bam' % input[0]]


###########################################################################
#
def merge(task_no, input):
    return ['%s_merged.bam' % input[0][0][:6]]

CHUNKS = 10
S2F_DURATION = 10
SPLIT_DURATION = 10
BWA_DURATION = 10
MERGE_DURATION = 10

gasw_repo = {

    "s2f.gasw": {
        "pre_exec": [
            "touch s2f.gasw",
            "curl -O https://raw.githubusercontent.com/marksantcroos/marvin/master/examples/solid2fastq.sh",
            "chmod +x solid2fastq.sh"
        ],
        # "executable": "tr",
        # "arguments": ['[:upper:]', '[:lower:]', '<', '${INPUT}', '>', '${INPUT}_fastq.txt'],
        "executable": "./solid2fastq.sh",
        "arguments": ['${INPUT}', 'fastq.gz', str(S2F_DURATION)],
        "output": ['fastq.gz'],
        # "post_function": s2f
    },

    "split.gasw": {
        "pre_exec": [
            "touch split.gasw",
            "curl -O https://raw.githubusercontent.com/marksantcroos/marvin/master/examples/split.sh",
            "chmod +x split.sh"
        ],
        # "executable": "split",
        # "arguments": ['-a', '1', '-l', str(int(math.ceil(26/float(CHUNKS)))), '${INPUT}', '${INPUT}_'],
        "executable": "./split.sh",
        "arguments": ['${INPUT}', str(CHUNKS), '${INPUT}_', str(SPLIT_DURATION)],
        "output": ['${INPUT}_%s' % suffix for suffix in string.ascii_lowercase[:CHUNKS]],
        "post_function": split
    },

    "bwa.gasw": {
        "pre_exec": [
            "touch bwa.gasw",
            "curl -O https://raw.githubusercontent.com/marksantcroos/marvin/master/examples/bwa.sh",
            "chmod +x bwa.sh"
        ],
        # "executable": "shuf",
        # "arguments": ['${INPUT}', '>', '${INPUT}.bam'],
        "executable": "./bwa.sh",
        "arguments": ['${INPUT}', '${INPUT}.bam', str(BWA_DURATION)],
        "output": ['${INPUT}.bam'],
        # "post_function": bwa
    },

    "merge.gasw": {
        "pre_exec": [
            "touch merge.gasw",
            "curl -O https://raw.githubusercontent.com/marksantcroos/marvin/master/examples/merge.sh",
            "chmod +x merge.sh"
        ],
        # "executable": "sort",
        # "arguments": ['-r', '*.bam', '>', 'result.bam'],
        "executable": "./merge.sh",
        "arguments": ['"*.bam"', 'result.bam', str(MERGE_DURATION)],
        "output": ['result.bam'],
        # "post_function": merge
    }
}