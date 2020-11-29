FROM jupyter/all-spark-notebook:latest

WORKDIR /code
COPY . /code
#USER root
#RUN echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
#RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
#RUN sudo apt-get update
#RUN apt-get install sbt
#RUN sudo apt-cache policy sbt
#USER joyvan

RUN pip install -r requirements.txt

