FROM python:3.7-alpine
ADD app.py /
RUN pip install flask
RUN pip install requests
EXPOSE 13800
CMD [ "python", "./app.py" ]