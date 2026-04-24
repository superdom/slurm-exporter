FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt pyproject.toml README.md ./
COPY slurm_exporter/ slurm_exporter/

RUN pip install --no-cache-dir .

EXPOSE 9410

ENTRYPOINT ["python", "-m", "slurm_exporter"]
CMD ["--port", "9410"]
