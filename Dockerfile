FROM python:alpine

# set a directory for the app
WORKDIR /usr/src/app

#copy pytho requirements
COPY requirements.txt .
# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# copy all the files to the container
COPY abb_fetch.py .

# run program
CMD ["python", "./abb_fetch.py"]