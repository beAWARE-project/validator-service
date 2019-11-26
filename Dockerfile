# Use an official Python runtime as a parent image
FROM python:3.7-slim

COPY src/ /usr/src/validator-service/

COPY requirements.txt /usr/src/validator-service/

# Set the working directory to /usr/src/validator-service/
WORKDIR /usr/src/validator-service/


# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip 
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run app.py when the container launches
CMD ["python3", "validator_module.py"]

