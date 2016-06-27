gasw_repo = {

    "s2f.gasw": {
        #"executable": "solid-to-fastqPE"
        "executable": "sleep",
        "arguments": ['1'],

    },

    "split.gasw": {
        # "executable": "split-fastq"
        "executable": "sleep",
        "arguments": ['2'],
    },

    "bwa.gasw": {
        # "executable": "bwa-short-paired-read",
        "executable": "sleep",
        "arguments": ['4'],
    },

    "merge.gasw": {
        # "executable": "merge-bam"
        "executable": "sleep",
        "arguments": ['3'],
    }
}