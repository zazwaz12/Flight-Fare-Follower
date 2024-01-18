FROM python:3.9

WORKDIR /app 

COPY /app .

COPY requirements.txt .

RUN pip install -r requirements.txt 

CMD ["python", "-m", "etl.pipelines.run"]

# run docker, the pipelines save the data to my localhost postgres 
# run docker, the logs dont get saved 

# run on local, both get saved 