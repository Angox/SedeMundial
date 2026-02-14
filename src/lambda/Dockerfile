FROM public.ecr.aws/lambda/python:3.11

# 1. Instalar compiladores (gcc) y librerías de desarrollo
# Amazon Linux 2023 usa 'dnf' en lugar de 'yum'
RUN dnf update -y && \
    dnf install -y gcc gcc-c++ python3-devel && \
    dnf clean all

COPY requirements.txt .

# 2. Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD [ "main.handler" ]
