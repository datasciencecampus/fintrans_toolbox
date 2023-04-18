# fintrans_toolbox

This repository contains useful functions and tools that if utilised will make life easier, with a lot of functions that have default or allow customisable inputs. These are particularly useful for interacting with big query. Thesea are stored in the src folder.

You will need to import the python modules into your notebook or script.



## Directory overview
- Data folder and all contents are unrtacked by GIT
- .gitignore default blocks popular data export file types due to data sensitivity
- Documents folder contains AQA plan, assumptions, caveats, project planning
- example_pipeline is from an older repo template and shows how to create a basic pipeline.
- Notebooks stores notebooks
- Outputs stores outputs
- src for functions, modules, python scripts
- Use .env for consistent file paths
- Test for tests of src and coverage
- Secrets require .secrets and are not tracked by Git, see Secrets and credentials for more information

See /docs/structure/README.md for an overview of the project structure, and docs/contributor_guide/README.md for an overview on contributing for developers.

## Getting started

```{warning}
Where this documentation refers to the root folder we mean where this README.md is
located.
```

### Requirements

- Access to Google Cloud Platform, Vertex AI Notebooks, Google Big Query
- Permissions to access project datasets and personal notebook
- Python 3.6.1+ installed
- a `.secrets` file (if needed to manage credentials) with the needed secrets and credentials
- load environment variables from `.env`

## Set-up

1) Create or clone a repositry using this template.

2) Install the Python requirements, open your terminal and enter:

```shell
pip install -r requirements.txt
```

3) Auto-setup pre-commit hooks and other features for development, open your terminal and enter:

```shell
make requirements
```

4) (Optional) Set-up .secrets and make template adjustments if required such as .gitignore, additional pre-commit hooks, additional directories.

5) Basic set-up complete. Once project documentation is complete you can create a site view of your documentaion by opening your terminal and entering:

```shell
make docs
```
  For code coverage open your terminal and run:

```shell
make coverage_html
```

## Loading environment variables

To load envrionment variables such as file paths from the `.env` hidden file you will need to use `dotenv` and `os` if using Python on windows.

1) Open your notebook and import modules
```python
from dotenv import load_dotenv
import os
```
2) Load `.env` file
```python
load_dotenv(override=True)
%env #returns all environment variables
```
3) Select the environment variable you require
```python
path_to_outputs = os.getenv("DIR_OUTPUTS")
print(path_to_outputs) #returns ./outputs
```

## Hidden Files & .Gitignore (if required)

For developers note that if using JuypterLabs that some files such as .gitignore are hidden by default. .Gitignore is set-up for both Python, R, and generally popular output methods. This limits the possibility to commit priveledged data to Git through various filetypes but this is not a replacement for best practice.

These hidden files are accesibly through GitHub or through changing JuypterLabs server config settings if required for individual users. This latter step is only recommended if developers require and is not recommended for standard or non-technical users. To enable hidden files:
1) To locate the jupyter config file, open the terminal and enter:
```shell
cd ~/.jupyter && nano jupyter_notebook_config.py
```
2) This will open the nano editor, under additional code at the bottom add this line
```shell
c.ContentsManager.allow_hidden = True
```
3) Press ctrl + O to save and hit enter to confirm the filename with agnostic file type saving. Press ctrl + x to exit.

4) Restart the notebook and kernals, any hidden . files will now be visible such as .secrets or .gitignore.

## Secrets and credentials (if required)

This is a recommended best practice to employ a `.secrets` file but may not be necessary for all projects where credential management is not required. For GCP Vertex AI Notebook work this step may be optional as credential management occurs at an earlier stage and external access is typically disallowed, users may wish to include project ID's and similar as secrets however.

To run this project, you need a `.secrets` file with secrets/credentials as environmental variables. The
secrets/credentials should have the following environment variable name(s):

| Secret/credential | Environment variable name | Description                                |
|-------------------|---------------------------|--------------------------------------------|
| Secret 1          | `SECRET_VARIABLE_1`       | Plain English description of Secret 1.     |
| Credential 1      | `CREDENTIAL_VARIABLE_1`   | Plain English description of Credential 1. |

Once added you can load these environment variables using `.env`. See docs/user_guide/loading_environment_variables.md for an overview.

## Loading secret variables (if required)

To load envrionment variables such as credentials from the `.secret` hidden file you will need to use `dotenv` and `os` if using Python on windows.

```{warning}
This is not the best practice solution and should be able to work without changing
directories or specifying the full directory similar to `.env` process.
Any contributions to fix this are welcome.
```

1) Create your `.secrets` file if it does not exist, open your terminal and type:
```shell
touch .secrets
```
2) Add your credentials into .secrets, this is untracked by GIT. Follow the format shown in Secrets and credentials (if required) section.

3) Open your notebook and import modules:
```python
from dotenv import load_dotenv
import os
```
2) Load `.secrets` file by either using the full file path or changing your directory to project root:
Using full file path:
```python
%pwd #this shows your full working directory, grab to the project root
load_dotenv("<full_directory_to_project_root>/.secrets", override=True)
%env #returns all environment variables
```

Alternatively change directory to root:
```python
#change directory to project root
cd ..
load_dotenv(".secrets", override=True)
%env #returns all environment variables
```
3) Select the environment variable you require
```python
my_secret = os.getenv("SECRET_1")
print(my_secret) #returns "yuki is a cute kitten"
```
