import string


def s2f(task_no, input):
    return ['fastq%d_%s' % (task_no, input)]

def split(task_no, input):
    chunks = 2
    return [['%s_slice_%s' % (input, letter)] for letter in string.ascii_uppercase[:chunks]]

def bwa(task_no, input):
    return ['%s.bam' % input[0]]

def merge(task_no, input):
    return ['%s_merged.bam' % input[0][0][:6]]


gasw_repo = {

    "s2f.gasw": {
        #"executable": "solid-to-fastqPE"
        "executable": "sleep",
        "arguments": ['1'],
        "function": s2f
    },

    "split.gasw": {
        # "executable": "split-fastq"
        "executable": "sleep",
        "arguments": ['1'],
        "function": split
    },

    "bwa.gasw": {
        # "executable": "bwa-short-paired-read",
        "executable": "sleep",
        "arguments": ['1'],
        "function": bwa
    },

    "merge.gasw": {
        # "executable": "merge-bam"
        "executable": "sleep",
        "arguments": ['1'],
        "function": merge
    }
}