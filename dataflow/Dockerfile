FROM apache/beam_python3.9_sdk:2.48.0

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "pipeline.py"]
