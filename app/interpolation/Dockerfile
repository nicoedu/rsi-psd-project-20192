FROM python:3.7.1-alpine3.7


COPY . .


COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt



CMD ["python","-u", "interpolationService.py"]