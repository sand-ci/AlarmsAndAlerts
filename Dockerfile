# FROM ivukotic/ml_platform:latest
FROM python:latest

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

RUN apt-get update && apt-get install -y \
    vim \
    sendmail \
    cmake 

COPY . .
RUN python3 -m pip install -r requirements.txt

RUN mkdir -p Users/Images
# build info
RUN echo "Timestamp:" `date --utc` | tee /image-build-info.txt

# CMD ["/.run"]
