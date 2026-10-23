FROM python:3.11-alpine
WORKDIR /app
COPY agent.py ./agent.py
ENV RT_SYNC_HUB=20.222.80.62
ENV RT_SYNC_PORT=443
CMD ["python3","agent.py","--hub","20.222.80.62","--name","ro01","--port","443","--pid-file","/tmp/.agent.pid"]
