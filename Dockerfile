FROM python:3.11-alpine 
WORKDIR /app
COPY . .
RUN pip install -r requirement.txt
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "python", "main.py"]
