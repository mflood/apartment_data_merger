FROM python:3.7
WORKDIR /app
ADD app /app
ADD data /data
RUN ls
RUN pwd
RUN pip install -r requirements.txt
RUN pip install -e .
CMD ["bash", "run.sh"]
