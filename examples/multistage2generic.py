#!/usr/bin/env python

from string import Template

class MultiTaskMultiStage():

    def __init__(self):

        self.tasks = []
        self.stages = []

        self.task_executable = None

        self.input_per_task_first_stage = []
        self.input_all_tasks_per_stage = []
        self.input_per_task_all_stages = []
        self.output_per_task_per_stage = []
        self.intermediate_per_task_per_stage = []
        self.output_per_task_final_stage = []

    def emulate(self):

        for t in self.tasks:
            for s in self.stages:
                print '\n##### Performing stage %s for task %s' % (s, t)

                # Input
                if s == self.stages[0]:
                    for f in self.input_per_task_first_stage:
                        tmp = Template(f)
                        f = tmp.substitute(TASK=t, STAGE=s)
                        print '### Reading initial input file %s' % f

                for f in self.input_all_tasks_per_stage:
                    tmp = Template(f)
                    f = tmp.substitute(TASK=t, STAGE=s)
                    print '### Reading all task per stage file %s' % f

                if s != self.stages[0]:
                    for f in self.intermediate_per_task_per_stage:
                        tmp = Template(f)
                        f = tmp.substitute(TASK=t, STAGE=s-1)
                        print '### Reading intermediate per task per stage file %s' % f

                for f in self.input_per_task_all_stages:
                    tmp = Template(f)
                    f = tmp.substitute(TASK=t, STAGE=s)
                    print '### Reading per task all stage file %s' % f

                if self.task_executable:
                    print '### Executing %s' % self.task_executable

                for f in self.output_per_task_per_stage:
                    tmp = Template(f)
                    f = tmp.substitute(TASK=t, STAGE=s)
                    print '### Writing output per task per stage file %s' % f

                if s != self.stages[-1]:
                    for f in self.intermediate_per_task_per_stage:
                        tmp = Template(f)
                        f = tmp.substitute(TASK=t, STAGE=s)
                        print '### Writing intermediate per task per stage file %s' % f

                if s == self.stages[-1]:
                    for f in self.output_per_task_final_stage:
                        tmp = Template(f)
                        f = tmp.substitute(TASK=t, STAGE=s)
                        print '### Writing output per task final stage file %s' % f


###############################################################################
#
# Main
#
if __name__ == '__main__':

    #
    # Application specific runtime characteristics
    #
    # Number of chromosomes
    NUM_CHRS = 1 # exp:5
    # Number of locations per chromosome
    NUM_LOCS = 2 # exp:21
    # Total number of systems to model, paper uses 105
    NUM_SYSTEMS = NUM_CHRS * NUM_LOCS
    # The time of simulation per system
    SIM_TRAJ_TIME = 3 # exp:20
    # The simulation time per dynamic step
    TASK_SIM_TIME = 1
    # The number of dynamic steps per system
    NUM_STEPS = SIM_TRAJ_TIME / TASK_SIM_TIME

    mtms = MultiTaskMultiStage()
    mtms.tasks = range(NUM_SYSTEMS)
    mtms.stages = range(NUM_STEPS)
    mtms.task_executable = 'namd-script.sh'

    #
    # INPUTS PER SYSTEM(S), FIRST DYNAMIC STEP(D) ONLY
    # (mineq_coor, mineq_vel, mineq_xsc)
    #     - mineq_coor[S]
    #     - mineq_vel[S]
    #     - mineq_xsc[S]
    #
    #
    mtms.input_per_task_first_stage = [
        'mineq-${TASK}.coor',
        'mineq-${TASK}.vel',
        'mineq-${TASK}.xsc'
    ]

    #
    # INPUTS PER DYNAMIC STEP(D) FOR ALL SYSTEMS(S) (conf)
    #     - conf_1 .. conf_D
    #
    mtms.input_all_tasks_per_stage = [
        'dyn-${STAGE}.conf'
    ]

    #
    # INPUTS PER SYSTEM(S) FOR ALL DYNAMIC STEPS(D)
    # (sys.pdb, sys.parm, sys.crc)
    #     - pdb[S]
    #     - parm[S]
    #     - crc[S]
    #
    mtms.input_per_task_all_stages = [
        'sys-${TASK}.pdb',
        'sys-${TASK}.parm',
        'sys-${TASK}.crc'
    ]

    #
    # SINKS PER SYSTEM(S) PER DYNAMIC STEP(D)
    # (dcd, cvd, xst, out, err)
    #     - dcd_1[S] .. dcd_D[S]
    #     - cvd_1[S] .. cvd_D[S]
    #     - xst_1[S] .. xst_D[S]
    #     - out_1[S] .. out_D[S]
    #     - err_1[S] .. err_D[S]
    #
    mtms.output_per_task_per_stage = [
        'dyn-${TASK}-${STAGE}.dcd',
        'dyn-${TASK}-${STAGE}.cvd',
        'dyn-${TASK}-${STAGE}.xst',
        'dyn-${TASK}-${STAGE}.out',
        'dyn-${TASK}-${STAGE}.err'
    ]

    #
    # INTERMEDIATE PER SYSTEM(S) PER DYNAMIC STEP(D)
    # (coor, vel, xsc)
    #     - coor_1[S] .. coor_D[S]
    #     - vel_1[S] .. vel_D[S]
    #     - xsc_1[S] .. xsc_D[S]
    #
    mtms.intermediate_per_task_per_stage = [
        'dyn-${TASK}-${STAGE}.coor',
        'dyn-${TASK}-${STAGE}.vel',
        'dyn-${TASK}-${STAGE}.xsc'
    ]

    #
    # OUTPUTS PER SYSTEM(S) FOR FINAL DYNAMIC STEP(D) ONLY
    # (coor, vel, xsc)
    #     - coor[S]
    #     - vel[S]
    #     - xsc[S]
    #
    mtms.output_per_task_final_stage = [
        'dyn-${TASK}-${STAGE}.coor',
        'dyn-${TASK}-${STAGE}.vel',
        'dyn-${TASK}-${STAGE}.xsc'
    ]

    mtms.emulate()

#
###############################################################################
