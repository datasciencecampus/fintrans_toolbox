import os
import shutil
import subprocess
import sys


def deactivate_venv():
    if "VIRTUAL_ENV" in os.environ:
        venv_name = os.environ["VIRTUAL_ENV"]
        print(f"Deactivating virtual environment at {venv_name}")
        # Deactivate the virtual environment
        deactivate_script = os.path.join(venv_name, "bin", "deactivate_this.py")
        if os.path.exists(deactivate_script):
            with open(deactivate_script) as f:
                exec(f.read(), {"__file__": deactivate_script})
        else:
            print("Deactivate script not found. Please deactivate manually.")
    else:
        print("No active virtual environment found.")


def delete_venv_and_kernel(venv_name):

    # Deactivate the virtual environment if active
    deactivate_venv()

    # Step 1: Delete the virtual environment directory
    if os.path.exists(venv_name):
        shutil.rmtree(venv_name)
        print(f"Virtual environment '{venv_name}' deleted successfully.")
    else:
        print(f"Virtual environment '{venv_name}' does not exist.")

    # Step 2: Delete the associated IPython kernel
    delete_kernel_command = f"jupyter kernelspec remove {venv_name} -f"
    subprocess.run(delete_kernel_command, shell=True, executable="/bin/bash")
    print(f"IPython kernel '{venv_name}' deleted successfully.")


# Check if the script is run with an argument for the virtual environment name
if len(sys.argv) != 2:
    print("Usage: python delete_env.py <venv_name>")
else:
    venv_name = sys.argv[1]
    # Run the function to delete venv and associated IPython kernel
    delete_venv_and_kernel(venv_name)
