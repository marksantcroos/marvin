import copy
import string
import radical.pilot as rp
import math


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
        dud.size = 1
        dud.selection = rp.SELECTION_FAST

        # TODO: Move into new directory?
        du = self.umgr.submit_data_units(dud, data_pilots=self.data_pilots, existing=True)

        self.output.append(du)


###########################################################################
#
def bwa(task_no, input):
    return ['%s.bam' % input[0]]


###########################################################################
#
def merge(task_no, input):
    return ['%s_merged.bam' % input[0][0][:6]]

chunks = 4

gasw_repo = {

    "s2f.gasw": {
        "executable": "tr",
        "arguments": ['[:upper:]', '[:lower:]', '<', '${INPUT}', '>', '${INPUT}_fastq.txt'],
        "output": ['${INPUT}_fastq.txt'],
        # "post_function": s2f
    },

    "split.gasw": {
        "executable": "split",
        "arguments": ['-a', '1', '-l', str(int(math.ceil(26/float(chunks)))), '${INPUT}', '${INPUT}_'],
        "output": ['${INPUT}_%s' % suffix for suffix in string.ascii_lowercase[:chunks]],
        "post_function": split
    },

    "bwa.gasw": {
        "executable": "shuf",
        "arguments": ['${INPUT}', '>', '${INPUT}.bam'],
        "output": ['${INPUT}.bam'],
        # "post_function": bwa
    },

    "merge.gasw": {
        "executable": "sort",
        "arguments": ['-r', '*.bam', '>', 'result.bam'],
        "output": ['result.bam'],
        # "post_function": merge
    }
}