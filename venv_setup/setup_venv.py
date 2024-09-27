import subprocess
import sys


def create_venv_and_install_requirements(venv_name):
    # Step 1: Create a virtual environment with the given name
    subprocess.run(["python", "-m", "venv", venv_name])

    # Step 2: Activate the virtual environment
    activate_command = f"source {venv_name}/bin/activate"

    # Step 3: Upgrade pip
    upgrade_pip_command = f"{activate_command} && pip install --upgrade pip"
    subprocess.run(upgrade_pip_command, shell=True, executable="/bin/bash")

    # Step 4: Install IPython kernel
    install_kernel_command = f"""
    {activate_command} && pip install ipykernel \\
    && python -m ipykernel install --user --name {venv_name} \\
    --display-name "Python - {venv_name}"
    """
    subprocess.run(install_kernel_command, shell=True, executable="/bin/bash")

    # Step 5: Verify the kernel installation
    verify_kernel_command = f"{activate_command} && jupyter kernelspec list"
    result = subprocess.run(
        verify_kernel_command,
        shell=True,
        executable="/bin/bash",
        capture_output=True,
        text=True,
    )

    if venv_name in result.stdout:
        print(f"IPython kernel '{venv_name}' installed successfully.")
    else:
        print(f"Failed to install IPython kernel '{venv_name}'.")


# Check if the script is run with an argument for the virtual environment name
if len(sys.argv) != 2:
    print("Usage: python setup_env.py <venv_name>")
else:
    venv_name = sys.argv[1]
    # Run the function to create venv and create IPython kernel
    create_venv_and_install_requirements(venv_name)

    # Print a success message
    print(
        f"""Virtual environment '{venv_name}' created,
        packages installed from requirements.txt
        and IPython kernel created successfully."""
    )
