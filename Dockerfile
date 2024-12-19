# stage # 1
FROM python:3.9-slim-bullseye as base
WORKDIR /opt/app
RUN /usr/local/bin/python -m pip install setuptools wheel
RUN pip config set global.no-cache-dir true
COPY ./requirements.txt /opt/app/requirements.txt
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /opt/app/wheels -r /opt/app/requirements.txt

# stage # 2
FROM python:3.9-slim-bullseye
RUN apt update && apt -y install
RUN /usr/local/bin/python -m pip install -U pip
WORKDIR /opt/app
RUN useradd -m appuser
USER appuser
ENV PATH="/home/appuser/.local/bin:$PATH"
COPY --chown=appuser:appuser --from=base /opt/app/wheels /opt/app/wheels/
RUN pip config set global.no-cache-dir true
RUN pip install --no-cache /opt/app/wheels/* --user

# copy every content from the local file to the image
WORKDIR /opt/app
COPY --chown=appuser:appuser ./migrationScript.py /opt/app/
COPY --chown=appuser:appuser ./start.sh /opt/app/
RUN chmod 700 /opt/app/start.sh

#adding entrypoint
ENTRYPOINT [ "/opt/app/start.sh" ]
