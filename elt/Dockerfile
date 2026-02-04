FROM python:3.12-slim

# Set the working directory
WORKDIR .

RUN apt-get update && apt-get install -y postgresql \
    postgresql-client \
    libpq-dev \
    gcc \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# RUN apt-get update && apt-get install -y psycopg2-binary \ 
#    sqlalchemy \
#    && rm -rf /var/lib/apt/lists/*

#COPY ./requirements.txt .
COPY elt_script.py .

#RUN pip install -r requirements.txt

CMD ["python", "elt_script.py"]