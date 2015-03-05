FROM centos

MAINTAINER noone

RUN ["yum", "install", "-y", "python-virtualenv"]
WORKDIR /kluge_scaler/
RUN ["virtualenv", "env"]
ADD requirements.txt /kluge_scaler/
RUN ["/kluge_scaler/env/bin/pip", "install", "-r", "/kluge_scaler/requirements.txt"]
ADD  . /kluge_scaler/
CMD /kluge_scaler/env/bin/python /kluge_scaler/monitor.py