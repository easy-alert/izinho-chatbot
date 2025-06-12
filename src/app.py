import os
import logging
from flask import Flask, request, jsonify
import vertexai
from vertexai.generative_models import GenerativeModel, Part
import sqlalchemy
from google.cloud.sql.connector import Connector

# --- CONFIGURAÇÃO LIDA DO AMBIENTE ---
app = Flask(__name__)

# --- CONFIGURAÇÃO DE LOGGING ---
# Configura o logger para imprimir no console. Cloud Run irá capturar isso.
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# GCP
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
LOCATION = os.environ.get("GCP_REGION")
AI_MODEL = os.environ.get("AI_MODEL", "gemini-2.0-flash-001")
vertexai.init(project=PROJECT_ID, location=LOCATION)

# Cloud SQL
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")

# --- INICIALIZAÇÃO DA CONEXÃO ---
connector = Connector()


def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
    )
    return conn


db_pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)


# --- PROMPT PARA A IA ---
# Este prompt é a parte mais importante para guiar a IA
PROMPT_TEMPLATE_SQL = """
Você é um assistente de banco de dados expert em PostgreSQL.
Sua tarefa é gerar uma query SQL a partir da pergunta de um usuário, seguindo regras estritas.
Gere APENAS a query SQL, sem nenhuma palavra ou explicação.

Contexto e Regras:
1.  **Regra de Sintaxe PostgreSQL CRÍTICA**: Todos os nomes de tabelas e colunas que contêm letras maiúsculas (camelCase) DEVEM ser colocados entre aspas duplas. Exemplo: a coluna `companyId` deve ser escrita como `"companyId"`.
2.  **Esquema do Banco de Dados**:
    - Tabela `buildings` com as colunas: "id" (TEXT), "buildingTypeId" (TEXT), "companyId" (TEXT), "name" (TEXT), "city" (TEXT), "state" (TEXT), "streetName" (TEXT), etc.
    - Tabela `users` com as colunas: "id" (TEXT), "name" (TEXT), "email" (TEXT), "role" (TEXT), "companyId" (TEXT), etc.
3.  **Regra de Segurança**: As queries DEVEM ser filtradas pelo `companyId` ou  `userId` fornecido para garantir que o usuário só veja dados da sua empresa ou relacionado a ele. Use sempre a cláusula `WHERE "companyId" = '{company_id}'` ou `WHERE "userId" = '{user_id}'`.
4.  **Exemplo de Query Correta**: `SELECT COUNT(*) FROM buildings WHERE buildings."companyId" = 'some-company-id';`

Pergunta do Usuário: "{question}"
ID da Empresa para o filtro: '{company_id}'
ID do Usuário para o filtro: '{user_id}'

Query SQL gerada:
"""

PROMPT_TEMPLATE_RESPONSE = """
Você é um assistente amigável. Sua tarefa é transformar um resultado de banco de dados em uma frase completa e natural para o usuário.
Pergunta original do usuário: "{question}"
Resultado do banco de dados: "{db_result}"

Resposta para o usuário:
"""

# --- MODELO DE IA ---
model = GenerativeModel(AI_MODEL)


@app.route("/health", methods=["GET"])
def health_check():
    """
    Este endpoint verifica a conectividade com o banco de dados.
    """
    try:
        # Pega uma conexão do pool. Se isso falhar, o bloco 'except' será acionado.
        conn = db_pool.connect()

        # Executa uma query extremamente simples que sempre funciona.
        conn.execute(sqlalchemy.text("SELECT 1"))

        # Fecha a conexão para devolvê-la ao pool.
        conn.close()

        # Se chegamos até aqui, a conexão foi bem-sucedida.
        return jsonify({"status": "ok", "database_connection": "successful"}), 200

    except Exception as e:
        # Se qualquer passo acima falhar, capturamos o erro específico.
        error_message = (
            f"Health check: FALHA na conexão com o banco de dados. Erro: {e}"
        )

        logging.error(f"ERRO CRÍTICO : {error_message}", exc_info=True)

        # Retornamos um erro 500 para indicar que o serviço não está saudável.
        return jsonify(
            {"status": "error", "database_connection": "failed", "details": str(e)}
        ), 500


@app.route("/chat", methods=["POST"])
def chat_handler():
    data = request.get_json()
    if (
        not data
        or "question" not in data
        or "user_id" not in data
        or "company_id" not in data
    ):
        return jsonify(
            {
                "error": "Parâmetros 'question', 'user_id' e 'company_id' são obrigatórios."
            }
        ), 400

    user_question = data["question"]
    user_id = data["user_id"]
    company_id = data["company_id"]

    try:
        logging.info("Iniciando o manipulador de chat.")
        # 1. Gerar a Query SQL com a IA
        prompt_sql = PROMPT_TEMPLATE_SQL.format(
            user_id=user_id, company_id=company_id, question=user_question
        )

        logging.debug("Prompt SQL formatado. Chamando a IA para gerar a query...")
        response_sql = model.generate_content([Part.from_text(prompt_sql)])
        sql_query = response_sql.text.strip()
        logging.info(f"Query gerada pela IA: '{sql_query}'")

        if not sql_query:
            logging.warning("A IA retornou uma query vazia.")

            return jsonify(
                {
                    "answer": f"Olá! Não consegui gerar uma busca para a sua pergunta: '{user_question}'. Como posso ajudar com os dados de seus prédios?"
                }
            )

        if not sql_query.upper().startswith("SELECT"):
            raise ValueError(
                f"Query insegura detectada (não inicia com SELECT): {sql_query}"
            )

        # 2. Executar a Query no Banco de Dados
        logging.info("Executando a query no banco de dados...")

        with db_pool.connect() as conn:
            result = conn.execute(sqlalchemy.text(sql_query))
            db_result = [row._asdict() for row in result]

        logging.debug(f"Resultado do banco de dados: {db_result}")

        # 3. Gerar a Resposta Final com a IA
        logging.info("Chamando a IA para formatar a resposta final...")
        prompt_response = PROMPT_TEMPLATE_RESPONSE.format(
            question=user_question, db_result=str(db_result)
        )
        response_final = model.generate_content([Part.from_text(prompt_response)])

        logging.info("Processo concluído com sucesso.")
        return jsonify({"answer": response_final.text.strip()})

    except Exception as e:
        logging.error(f"ERRO CRÍTICO no manipulador de chat: {e}", exc_info=True)

        return jsonify(
            {
                "error": "Desculpe, não consegui processar sua pergunta. Tente reformulá-la."
            }
        ), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
