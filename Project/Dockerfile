FROM akhiljay/cc_images:first

RUN mkdir -p /code
COPY ./requirements.txt /code/requirements.txt
COPY ./merged_master.py /code/merged_master.py
COPY ./merged_slave.py /code/merged_slave.py
COPY ./Master.py /code/Master.py
COPY ./Slave.py /code/Slave.py
COPY ./worker.py /code/worker.py
COPY ./Orchestrator.py /code/Orchestrator.py
COPY ./setenv.py /code/setenv.py
COPY ./shO.sh /code/shO.sh
COPY ./shM.sh /code/shM.sh
COPY ./shS.sh /code/shS.sh
RUN pip3 install -r /code/requirements.txt
COPY . /code
WORKDIR /code
