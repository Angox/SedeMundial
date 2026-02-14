FROM public.ecr.aws/lambda/python:3.11

# 1. Copiamos requirements primero para aprovechar la caché de Docker
COPY requirements.txt .

# 2. Actualizamos PIP e instalamos las dependencias usando SOLO binarios.
# --only-binary=:all: fuerza a pip a fallar si intenta compilar, 
# pero como hemos elegido versiones estables, encontrará los binarios.
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --only-binary=:all: -r requirements.txt

# 3. Copiamos el código
COPY main.py .

# 4. Comando de arranque
CMD [ "main.handler" ]
