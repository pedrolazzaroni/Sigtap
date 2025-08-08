import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_DATABASE', 'sigtap')
DB_USER = os.getenv('DB_USERNAME', 'root')
DB_PASS = os.getenv('DB_PASSWORD', '')

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4")

# Lista todas as tabelas
with engine.connect() as conn:
    tables = [row[0] for row in conn.execute(text("SHOW TABLES")).fetchall()]

if not tables:
    print("Nenhuma tabela encontrada no banco.")
    exit(1)

# Exporta cada tabela para uma aba do Excel
excel_path = "sigtap_export.xlsx"
with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
    for table in tables:
        print(f"Exportando {table}...")
        df = pd.read_sql_table(table, engine)
        df.to_excel(writer, sheet_name=table[:31], index=False)  # Excel limita nome da aba a 31 chars
print(f"Exportação concluída: {excel_path}")
