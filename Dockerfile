# Use an official Python runtime as a parent image
FROM python:3.7-slim

# Set the working directory to /kbs_workdir
WORKDIR /val_workdir

# Copy the current directory contents into the container at /app
COPY ./Validator /val_workdir

# Copy the current directory contents into the container at /app
COPY ./requirements.txt /val_workdir

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip 
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run app.py when the container launches
CMD ["python3", "validator_module.py"]

