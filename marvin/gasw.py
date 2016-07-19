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
        # "executable": "sleep",
        # "arguments": ['1'],
        # tr "[:upper:]" "[:lower:]" < solid.txt > fastq.txt
        "executable": "tr",
        # "arguments": ['[:upper:]', '[:lower:]', '<', 'solid.txt', '>', 'fastq.txt'],
        "arguments": ['[:upper:]', '[:lower:]', '<', '${TARGET}', '>', 'fastq.txt'],
        # "input": 'solid.txt',
        "output": 'fastq.txt',
        "function": s2f
    },

    "split.gasw": {
        # "executable": "split-fastq"
        # "executable": "sleep",
        # "arguments": ['1'],
        # split -l4 fastq.txt fastq.
        #  awk 'NR%4==1 { file = "fastq_" sprintf("%04d", NR/4) } { print > file }' fastq.txt
        "executable": "awk",
        "arguments": ['NR%13==1 { file = "fastq_" sprintf("%04d", NR/13) } { print > file }', 'fastq.txt'],
        "input": 'fastq.txt',
        "output": 'fastq_0000',
        "function": split
    },

    "bwa.gasw": {
        # "executable": "bwa-short-paired-read",
        # "executable": "sleep",
        # "arguments": ['1'],
        # gshuf fast_0000 > fast_0000.bam
        "executable": "gshuf",
        "arguments": ['fastq_0000', '>', 'fastq_0000.bam'],
        "input": 'fastq_0000',
        "output": 'fastq_0000.bam',
        "function": bwa
    },

    "merge.gasw": {
        # "executable": "merge-bam"
        # "executable": "sleep",
        # "arguments": ['1'],
        # sort -r *.bam > result.bam
        "executable": "sort",
        "arguments": ['-r', '*.bam', '>', 'result.bam'],
        "input": 'fastq_0000.bam',
        "output": 'result.bam',
        "function": merge
    }
}