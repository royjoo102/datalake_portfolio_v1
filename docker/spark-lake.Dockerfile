FROM spark:3.5.8
USER root
COPY requirements.txt requirements-lake.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt -r /tmp/requirements-lake.txt
USER 185
WORKDIR /workspace
