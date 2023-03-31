# Use the gdal:ubuntu-full-latest base image
FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y cdo python3-pip

# Install pip
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel

# set work directory
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies
RUN pip install --upgrade pip setuptools wheel
RUN pip install --upgrade pip
COPY ./requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt

# copy project
COPY . /usr/src/app/

#run docker-entrypoint.sh
ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]