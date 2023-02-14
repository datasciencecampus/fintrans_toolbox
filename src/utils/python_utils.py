import shutil

def delete_folder(folder_name):
    """Deletes folder as you canot do it from GCP notebooks manually.
    example: delete_folder('fintrans_toolbox')"""
    shutil.rmtree(folder_name)