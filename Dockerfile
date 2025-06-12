# Use uma imagem base oficial do Python
FROM python:3.9-slim

# Defina o diretório de trabalho dentro do container
WORKDIR /app

# Copie o arquivo de dependências
COPY requirements.txt .

# Instale as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copie o resto do código do seu aplicativo
COPY . .

# Comando para iniciar o servidor web
CMD gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0 src.app:app
