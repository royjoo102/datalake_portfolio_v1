FROM spark:3.5.8
USER root
RUN pip3 install --no-cache-dir PyYAML==6.0.2
USER 185
WORKDIR /workspace
