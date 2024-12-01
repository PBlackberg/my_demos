'''
# -----------------
#   my_settings
# -----------------
Specify gadi username and projects
'''

import os

def get_user_specs():
    username = os.path.expanduser("~").split('/')[-1]                               # 'cb4968'
    scratch_project = 'w40'                                                         # 'k10'
    SU_project = 'k10'                                                              #
    data_projects = ('hh5', 'al33', 'oi10', 'ia39', 'rt52', 'fs38')                 # []
    folder_scratch = (f'/scratch/{scratch_project}/{username}')                     # I recommend to use scratch folder normally
    # folder_scratch = (os.getcwd())                                                  # you can also use current working directory for these examples if you like
    return username, SU_project, data_projects, scratch_project, folder_scratch


if __name__ == '__main__':
    username, SU_project, data_projects, scratch_project, folder_scratch = get_user_specs()
    [print(f) for f in [username, SU_project, data_projects, scratch_project, folder_scratch]]



