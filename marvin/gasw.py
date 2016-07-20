import copy
import string
import radical.pilot as rp


def s2f(task_no, input):
    return ['fastq%d_%s' % (task_no, input)]


def split(self):
    # chunks = 2
    # return [['%s_slice_%s' % (self.input, letter)] for letter in string.ascii_uppercase[:chunks]]

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


def bwa(task_no, input):
    return ['%s.bam' % input[0]]


def merge(task_no, input):
    return ['%s_merged.bam' % input[0][0][:6]]


gasw_repo = {

    "s2f.gasw": {
        "executable": "tr",
        "arguments": ['[:upper:]', '[:lower:]', '<', '${INPUT}', '>', 'fastq.txt'],
        "output": ['fastq.txt'],
        # "post_function": s2f
    },

    "split.gasw": {
        "executable": "awk",
        "arguments": ['NR%13==1 { file = "fastq_" sprintf("%04d", NR/13) } { print > file }', 'fastq.txt'],
        "output": ['fastq_0000', 'fastq_0001'],
        "post_function": split
    },

    "bwa.gasw": {
        "executable": "gshuf",
        "arguments": ['${INPUT}', '>', '${INPUT}.bam'],
        "output": ['${INPUT}.bam'],
        # "output": ['fastq_0000.bam'],
        # "post_function": bwa
    },

    "merge.gasw": {
        "executable": "sort",
        "arguments": ['-r', '*.bam', '>', 'result.bam'],
        "output": ['result.bam'],
        # "post_function": merge
    }
}