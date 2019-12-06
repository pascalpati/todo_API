#The dockerfile defines the image's environment

# import python runtime and set-up working directory
FROM python:3.7-alpine
WORKDIR /main
ADD . /main

# install all necessary dependencies (as specified in requirements.txt)
RUN pip install -r requirements.txt

# open port 80 for serving the web app
EXPOSE 80

# run the app when container launches
CMD ["python", "main.py"]



