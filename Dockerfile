#define python version
FROM python:3.7

COPY requirements.txt .
RUN pip install -r requirements.txt

## Get necessary files
COPY python_scripts/*.py python_scripts/

COPY meta_data/ meta_data/ 
COPY glue_jobs/ glue_jobs/

ENTRYPOINT python -u python_scripts/$PYTHON_SCRIPT_NAME