 
FROM public.ecr.aws/lambda/python:3.12

WORKDIR ${LAMBDA_TASK_ROOT}

# Copy requirements.txt cloud aws
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# dbt project folder
COPY ./dbt ${LAMBDA_TASK_ROOT}/dbt

# allow rw
RUN chmod 755 -R ${LAMBDA_TASK_ROOT}/dbt/lastfm

# aws - lastfm ev vars -> secrets

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
#CMD [ "lambda_function.handler" ]
ENTRYPOINT [ "python", "./lambda_function.py"]
