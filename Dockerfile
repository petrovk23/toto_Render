# Dockerfile

# Pin Python to a known stable version
ARG PYTHON_VERSION=3.10-slim
FROM python:${PYTHON_VERSION}

LABEL fly_launch_runtime="flask"

WORKDIR /code

# 1) Upgrade pip, setuptools, wheel to avoid build issues
RUN pip install --upgrade pip setuptools wheel

# 2) Copy in your requirements and install
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 3) Copy the rest of your app code
COPY . .

# 4) Expose port 8080 on the container
EXPOSE 8080

# 5) Use Gunicorn to run the Flask app in production.
#    Adjust 'app:app' to match your Flask instance filename & variable, e.g. "app.py" => "app"
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]
