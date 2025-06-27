import os
import logging
import re
import sqlalchemy
import vertexai
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud.sql.connector import Connector
from vertexai.generative_models import GenerativeModel, Part
from cachetools import cached, TTLCache

# --- CONFIGURAÇÃO LIDA DO AMBIENTE ---
app = Flask(__name__)
CORS(app)  # Habilita CORS para permitir requisições de outros domínios

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


@cached(cache=TTLCache(maxsize=1, ttl=600))  # <-- MÁGICA DO CACHE AQUI!
def get_dynamic_schema():
    """
    Conecta ao banco, busca o esquema das tabelas e formata para o prompt.
    O resultado é cacheado por 600 segundos (10 minutos) para evitar sobrecarga.
    """
    logging.info("Buscando esquema do banco de dados (chamada não cacheada)...")
    schema_description = ""

    try:
        with db_pool.connect() as conn:
            # Query que busca tabelas e colunas do schema 'public'
            query = sqlalchemy.text("""
                SELECT table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                ORDER BY table_name, ordinal_position;
            """)
            result = conn.execute(query)

            # Formata o resultado para o prompt
            current_table = ""
            for row in result:
                row_dict = row._asdict()
                if row_dict["table_name"] != current_table:
                    current_table = row_dict["table_name"]
                    schema_description += f"\n- Tabela `{current_table}` com colunas:"
                schema_description += (
                    f' "{row_dict["column_name"]}" ({row_dict["data_type"]}),'
                )

            logging.info("Esquema gerado com sucesso.")
            return schema_description

    except Exception as e:
        logging.error(f"Falha ao gerar o esquema dinâmico do banco: {e}")
        return "Erro ao obter esquema do banco."


def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME, "pg8000", user=DB_USER, password=DB_PASS, db=DB_NAME
    )
    return conn


db_pool = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)


# --- PROMPT PARA A IA ---
# Este prompt é a parte mais importante para guiar a IA
PROMPT_TEMPLATE_SQL = """
Você é um assistente de banco de dados expert em PostgreSQL.
Sua tarefa é gerar uma query SQL a partir da pergunta de um usuário, seguindo regras estritas.
Gere APENAS a query SQL, sem nenhuma palavra ou explicação.

---
HISTÓRICO DA CONVERSA:
{chat_history}
---

Contexto e Regras:
1.  **Regra de Sintaxe PostgreSQL CRÍTICA**: Todos os nomes de tabelas e colunas que contêm letras maiúsculas (camelCase) DEVEM ser colocados entre aspas duplas. Exemplo: `"companyId"`.
2.  **Esquema do Banco de Dados**: {db_schema}
3.  **Relações Importantes (Exemplo, adicione as suas)**:
    - Se precisar cruzar dados de prédios com tarefas, use `JOIN buildings ON tasks."buildingId" = buildings.id`.
4.  **Regra de Segurança**: As queries DEVEM ser filtradas pelo `companyId` ou  `userId` fornecido para garantir que o usuário só veja dados da sua empresa ou relacionado a ele. Use sempre a cláusula `WHERE "companyId" = '{company_id}'` ou `WHERE "userId" = '{user_id}'`.
5.  **Exemplo de Query Correta**: `SELECT COUNT(*) FROM buildings WHERE buildings."companyId" = 'some-company-id';`

Pergunta MAIS RECENTE do Usuário: "{question}"
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

    if not all(k in data for k in ["question", "user_id", "company_id"]):
         return jsonify({"error": "Parâmetros obrigatórios ausentes."}), 400

    user_question = data["question"]
    user_id = data["user_id"]
    company_id = data["company_id"]
    history = data.get("history", []) 
    session_id = data.get("session_id", "no-session")

    try:
        logging.info(f"Iniciando chat para sessão: {session_id}")

        # 1. Pega o schema do banco (do cache ou buscando novamente)
        db_schema = get_dynamic_schema()

        # 2. Construa a string do histórico da conversa
        chat_history_str = ""
        for message in history:
            sender = "Usuário" if message.get("sender") == "user" else "Assistente"
            chat_history_str += f"{sender}: {message.get('text')}\n"

        # 3. Formate o prompt com o histórico
        prompt_sql = PROMPT_TEMPLATE_SQL.format(
            chat_history=chat_history_str,
            db_schema=db_schema,
            company_id=company_id,
            user_id=user_id,
            question=user_question
        )

        logging.debug("Prompt SQL formatado. Chamando a IA para gerar a query...")
        response_sql = model.generate_content([Part.from_text(prompt_sql)])

        raw_response = response_sql.text
        logging.info(f"Resposta bruta da IA: '{raw_response}'")

        # 4. Tenta extrair a query de dentro de um bloco de código Markdown
        match = re.search(r"```(sql)?(.*)```", raw_response, re.DOTALL | re.IGNORECASE)
        if match:
            sql_query = match.group(2).strip()
        else:
            # Se não houver Markdown, assume que a resposta inteira é a query
            sql_query = raw_response.strip()

        logging.info(f"Query extraída e limpa: '{sql_query}'")

        if not sql_query:
            logging.warning("A IA retornou uma query vazia após a limpeza.")
            return jsonify(
                {
                    "answer": "Desculpe, não consegui formular uma busca com sua pergunta."
                }
            )

        if not sql_query.upper().startswith("SELECT"):
            raise ValueError(
                f"Query insegura ou mal formada detectada após a limpeza: {sql_query}"
            )

        # 5. Executar a Query no Banco de Dados
        logging.info("Executando a query no banco de dados...")

        with db_pool.connect() as conn:
            result = conn.execute(sqlalchemy.text(sql_query))
            db_result = [row._asdict() for row in result]

        logging.debug(f"Resultado do banco de dados: {db_result}")

        # 6. Gerar a Resposta Final com a IA
        logging.info("Chamando a IA para formatar a resposta final...")
        prompt_response = PROMPT_TEMPLATE_RESPONSE.format(
            question=user_question, db_result=str(db_result)
        )
        response_final = model.generate_content([Part.from_text(prompt_response)])

        logging.info("Processo concluído com sucesso.")
        return jsonify({"answer": response_final.text.strip()})

    except Exception as e:
        logging.error(f"ERRO CRÍTICO na sessão {session_id}: {e}", exc_info=True)
        return jsonify({"error": "Desculpe, não consegui processar sua pergunta."}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
