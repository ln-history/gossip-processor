# 1. Use an official Python runtime as a parent image
FROM python:3.13-slim

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy requirements and install them
# (We assume you have pyzmq and python-dotenv in requirements.txt)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy the current code
COPY main.py .

# 5. Run the consumer script with unbuffered output (-u)
CMD ["python", "-u", "main.py"]