def s2f(task_no):
    return ['fastq%d' % task_no]

def split(task_no):
    return [['fastq_slice_A'], ['fastq_slice_B']]

def bwa(task_no):
    return ['bam_%d' % task_no]

def merge(task_no):
    return ['bam_merged_%d' % task_no]


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