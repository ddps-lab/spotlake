FROM amazon/aws-lambda-python:3.8

# optional : ensure that pip is up to data
RUN /var/lang/bin/python3.8 -m pip install --upgrade pip

# install git 
RUN yum install git -y
RUN yum install wget -y

# download spotinfo
RUN wget https://github.com/alexei-led/spotinfo/releases/download/1.0.7/spotinfo_linux_amd64 -O spotinfo -P /var/task/
RUN chmod +x /var/task/spotinfo

# git clone >>>> link to github of odobenuskr, need to change
RUN git clone https://github.com/odobenuskr/spotinfo-lambda.git

# move lambdafunc.py
RUN cp spotinfo-lambda/lambda_function.py /var/task/

CMD ["lambda_function.handler"]
