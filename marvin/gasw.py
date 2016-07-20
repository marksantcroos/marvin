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
    print 'files: %s' % orig.description.file_urls
    for file in orig.description.file_urls:
        dud = rp.DataUnitDescription()
        dud.name = file
        dud.file_urls = [file]
        dud.size = 1
        dud.selection = rp.SELECTION_FAST

        du = self.umgr.submit_data_units(dud, data_pilots=self.data_pilots, existing=False)

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
        "executable": "gshuf",
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