'''
# ===========================
    qsub_parallelization
# ===========================
The example-folders contain examples of calculating a metric on sections of data in parallel using the qsub scheduler

# -- folder structure --
example_folder
    job_submission.py   - Submits copies of section_calc.py to nodes, where each node gets information to help identify the relevant section
    calc_script         - Picks out relevant section, executes calculation, and saves result of node
    show_result         - Loads saved results from nodes and prints result
utils
    my_settings.py      - Specifies username and general job-submission settings (python version, project folder, scratch_folder, etc.)
    submission_funcs.py - Functions to create resource script, submit job, and check job status

# -- Workflow --
First, resource scripts for each node are created and put in an oe_folder (node terminal output folder) (folder_scratch specified in my_settings.py)
When the job is completed, a node terminal output file (oe_file) with the same name as the resource script is added to the oe_folder
If the job is executed without error, the resource script is deleted. If the job had an error, the resource script remains (making it easier to see which job failed, if many jobs are submitted)
The result from each node is saved as separate files in a result_folder (in folder_scratch specified in my_settings.py)
After the results have been calculated, the oe_folder can be deleted manually

# -- Test --
To test the example, 
- check the settings in utils/util_qsub/my_settings.py match your specifications
- execute job_submission.py in the terminal
- execute show_results.py when the jobs are finished, to see the result
The utils folder needs to be in your current working directory for the execution to work as expected.

GL

'''

