import os
from flask import Flask, request, jsonify
import vertexai
from vertexai.generative_models import GenerativeModel, Part
import sqlalchemy
from google.cloud.sql.connector import Connector

# --- CONFIGURAÇÃO LIDA DO AMBIENTE ---
app = Flask(__name__)

# GCP
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
LOCATION = os.environ.get("GCP_REGION")
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


# --- TESTE DE CONEXÃO NA INICIALIZAÇÃO ---
print("Realizando teste de conexão inicial com o banco de dados...")
try:
    with db_pool.connect() as conn:
        conn.execute(sqlalchemy.text("SELECT 1"))
    print("CONEXÃO COM O BANCO DE DADOS BEM-SUCEDIDA NA INICIALIZAÇÃO.")
except Exception as e:
    print(f"FALHA FATAL NA CONEXÃO COM O BANCO NA INICIALIZAÇÃO: {e}")
    # Opcional: Você pode querer que a aplicação pare aqui se o banco não estiver acessível.
    # Em um ambiente serverless como o Cloud Run, logar o erro já é suficiente
    # para que o container seja marcado como "não-saudável".


# --- PROMPT PARA A IA ---
# Este prompt é a parte mais importante para guiar a IA
PROMPT_TEMPLATE_SQL = """
Você é um assistente de banco de dados expert em PostgreSQL.
Sua tarefa é gerar uma query SQL a partir da pergunta de um usuário.
Gere APENAS a query SQL, sem nenhuma outra palavra, explicação ou ```sql.

Contexto:
- O esquema do banco de dados é o seguinte:
  - Tabela buildings (nome inferido) com as colunas: id (TEXT), buildingTypeId (TEXT), companyId (TEXT), name (TEXT), cep (TEXT), city (TEXT), state (TEXT), neighborhood (TEXT), streetName (TEXT), area (TEXT), deliveryDate (TIMESTAMP), warrantyExpiration (TIMESTAMP), keepNotificationAfterWarran (BOOLEAN), createdAt (TIMESTAMP), updatedAt (TIMESTAMP), nanoId (TEXT), mandatoryReportProof (BOOLEAN), residentPassword (TEXT), syndicPassword (TEXT), nextMaintenanceCreationBas (public."NextMa..."), isActivityLogPublic (BOOLEAN), guestCanCompleteMaintena (BOOLEAN), image (TEXT), isBlocked (BOOLEAN).
  - Tabela users com as colunas: id (TEXT), name (TEXT), email (TEXT), isBlocked (BOOLEAN), passwordHash (TEXT), lastAccess (TIMESTAMP), createdAt (TIMESTAMP), updatedAt (TIMESTAMP), emaillsConfirmed (BOOLEAN), image (TEXT), isMainContact (BOOLEAN), phoneNumber (TEXT), phoneNumberlsConfirmed (BOOLEAN), role (TEXT), showContact (BOOLEAN), lastNotificationDate (TIMESTAMP), colorScheme (TEXT).
- O usuário com ID '{user_id}', vinculado a uma empresa específica ID '{company_id}', fez a pergunta.
- **REGRA DE SEGURANÇA CRÍTICA**: Todas as queries DEVEM conter a cláusula `WHERE userId = '{user_id}'` ou `WHERE companyId = '{company_id}'` para garantir que o usuário só veja seus próprios dados, ou dados relacionados a sua empresa.
- Se a pergunta for sobre "quantos", use `COUNT(*)`.

Pergunta do Usuário: "{question}"

Query SQL gerada:
"""

PROMPT_TEMPLATE_RESPONSE = """
Você é um assistente amigável. Sua tarefa é transformar um resultado de banco de dados em uma frase completa e natural para o usuário.
Pergunta original do usuário: "{question}"
Resultado do banco de dados: "{db_result}"

Resposta para o usuário:
"""

# --- MODELO DE IA ---
model = GenerativeModel(
    "gemini-1.5-flash-001"
)  # Usamos o Gemini 1.5 Flash (rápido e econômico)


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
        print("Health check: Conexão com o banco de dados bem-sucedida!")
        return jsonify({"status": "ok", "database_connection": "successful"}), 200

    except Exception as e:
        # Se qualquer passo acima falhar, capturamos o erro específico.
        error_message = (
            f"Health check: FALHA na conexão com o banco de dados. Erro: {e}"
        )
        print(error_message)

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
        print("DEBUG: Iniciando o manipulador de chat.")
        # 1. Gerar a Query SQL com a IA
        prompt_sql = PROMPT_TEMPLATE_SQL.format(
            user_id=user_id, company_id=company_id, question=user_question
        )

        print("DEBUG: Prompt SQL formatado. Chamando a IA para gerar a query...")
        response_sql = model.generate_content([Part.from_text(prompt_sql)])
        sql_query = response_sql.text.strip()
        print(f"DEBUG: Query gerada pela IA: '{sql_query}'")

        if not sql_query:
            print(
                "DEBUG: A IA retornou uma query vazia. Respondendo de forma amigável."
            )
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
        print("DEBUG: Executando a query no banco de dados...")
        with db_pool.connect() as conn:
            result = conn.execute(sqlalchemy.text(sql_query))
            db_result = [row._asdict() for row in result]
        print(f"DEBUG: Resultado do banco de dados: {db_result}")

        # 3. Gerar a Resposta Final com a IA
        prompt_response = PROMPT_TEMPLATE_RESPONSE.format(
            question=user_question, db_result=str(db_result)
        )
        print("DEBUG: Chamando a IA para formatar a resposta final...")
        response_final = model.generate_content([Part.from_text(prompt_response)])

        print("DEBUG: Processo concluído com sucesso.")
        return jsonify({"answer": response_final.text.strip()})

    except Exception as e:
        # ESTE é o print que precisamos ver nos logs
        print(f"ERRO CRÍTICO no manipulador de chat: {e}")
        return jsonify(
            {
                "error": "Desculpe, não consegui processar sua pergunta. Tente reformulá-la."
            }
        ), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
