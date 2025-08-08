#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carrega arquivos de competências do SIGTAP (TXT/CSV/ZIP/DBF) para MySQL.

- Detecta automaticamente codificação (UTF-8, latin-1, cp1252) e delimitador (| ; , \t)
- Aceita pastas contendo várias competências e arquivos ZIP (extrai para temporário)
- Cria o banco de dados se não existir (padrão: SIgtap)
- Insere os dados em tabelas nomeadas a partir do arquivo (com sufixo de competência se detectado)
- Adiciona uma coluna 'competencia' (AAAAMM) quando conseguida por pasta/arquivo

Requisitos: pandas, sqlalchemy, pymysql, chardet, dbfread, python-dotenv (opcional)
"""

from __future__ import annotations
import argparse
import os
import re
import sys
import zipfile
import tempfile
import shutil
import logging
from pathlib import Path
from typing import Iterable, Optional, Tuple, List
from dataclasses import dataclass

# Dependências de runtime
try:
    import pandas as pd
except Exception:
    print("Erro ao importar pandas. Instale dependências com: pip install -r requirements.txt", file=sys.stderr)
    raise

try:
    from sqlalchemy import create_engine, text
except Exception:
    print("Erro ao importar SQLAlchemy. Instale dependências com: pip install -r requirements.txt", file=sys.stderr)
    raise

# Drivers/auxiliares
try:
    import chardet  # type: ignore
except Exception:
    chardet = None

try:
    from dbfread import DBF  # type: ignore
except Exception:
    DBF = None

# .env opcional
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    def load_dotenv(*args, **kwargs):
        return False


# ------------------------ Utilidades -------------------------

def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='[%(levelname)s] %(message)s'
    )


def detect_competencia_from_path(path: Path) -> Optional[str]:
    """Tenta extrair AAAAMM do nome de pasta/arquivo."""
    patterns = [
        r"(?P<comp>20\d{2}[01]\d)",  # 200001-209912
        r"(?P<comp>\d{6})",
    ]
    s = str(path)
    for pat in patterns:
        m = re.search(pat, s)
        if m:
            comp = m.group('comp')
            # valida MM
            if 1 <= int(comp[-2:]) <= 12:
                return comp
    return None


def detect_encoding(sample_bytes: bytes) -> str:
    if chardet is not None:
        res = chardet.detect(sample_bytes)
        enc = (res.get('encoding') or '').lower()
        if enc:
            return enc
    # Fallbacks comuns no SIGTAP
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            sample_bytes.decode(enc)
            return enc
        except Exception:
            continue
    return "utf-8"


def detect_delimiter(sample_text: str) -> str:
    # Ordem comum: pipe | ; , \t
    candidates = ['|', ';', ',', '\t']
    counts = {d: sample_text.count(d) for d in candidates}
    # Escolhe o que tiver mais incidência
    delim = max(counts, key=counts.get)
    # Evita falso positivo: se todos zero, assume pipe
    if all(c == 0 for c in counts.values()):
        return '|'
    return delim


def sanitize_table_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip('_')
    # Limite do MySQL = 64
    return name[:64]


def strip_competencia_suffix(stem: str) -> str:
    """Remove sufixo _AAAAMM do nome (se houver)."""
    m = re.match(r"^(.*)_(\d{6})$", stem)
    if m and 1 <= int(m.group(2)[-2:]) <= 12:
        return m.group(1)
    return stem


# ------------------------ Banco de Dados ---------------------

def ensure_database(host: str, port: int, user: str, password: str, db_name: str) -> None:
    # Conecta sem banco específico e cria se necessário
    root_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/mysql"
    engine = create_engine(root_url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
        conn.commit()


def make_engine(host: str, port: int, user: str, password: str, db_name: str):
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}?charset=utf8mb4"
    engine = create_engine(url, pool_pre_ping=True)
    return engine


# ------------------------ Leitura de Arquivos ----------------

def read_tabular_file(path: Path, delimiter: Optional[str], encoding: Optional[str], header: Optional[int] = 'infer', names: Optional[List[str]] = None) -> pd.DataFrame:
    # Lê um arquivo CSV/TXT com detecção básica
    # Amostra
    with open(path, 'rb') as f:
        sample = f.read(100_000)
    enc = encoding or detect_encoding(sample)
    text_sample = sample.decode(enc, errors='ignore')
    sep = delimiter or detect_delimiter(text_sample)

    # Tenta ler com header na primeira linha, preservando strings
    df = pd.read_csv(
        path,
        sep=sep,
        encoding=enc,
        dtype=str,
        keep_default_na=False,
        na_values=[],
        low_memory=False,
        on_bad_lines='skip',
        header=header,
        names=names
    )
    # Normaliza nomes de colunas
    df.columns = [sanitize_table_name(str(c)) or f"col_{i}" for i, c in enumerate(df.columns)]
    return df


def read_dbf_file(path: Path, encoding: Optional[str]) -> pd.DataFrame:
    if DBF is None:
        raise RuntimeError("Pacote 'dbfread' não instalado. Adicione ao requirements e instale.")
    enc = encoding or 'latin-1'
    table = DBF(str(path), encoding=enc, ignore_missing_memofile=True)
    rows = list(table)
    df = pd.DataFrame(rows, dtype=str)
    if df.empty and len(table.field_names) > 0:
        # Garante colunas mesmo vazio
        df = pd.DataFrame(columns=[sanitize_table_name(c) for c in table.field_names])
    df.columns = [sanitize_table_name(str(c)) for c in df.columns]
    return df


# ------------------------ Pipeline Principal -----------------

def iter_supported_files(root: Path) -> Iterable[Path]:
    for p in root.rglob('*'):
        if p.is_file() and p.suffix.lower() in {'.txt', '.csv', '.zip', '.dbf'}:
            yield p


def extract_zip_to_temp(zip_path: Path) -> Path:
    tmpdir = Path(tempfile.mkdtemp(prefix='sigtap_zip_'))
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(tmpdir)
    return tmpdir


# ------------------------ Layouts/Migrations -----------------

class ColumnSpec:
    def __init__(self, name: str, mysql_type: str, nullable: bool = True):
        self.name = sanitize_table_name(name)
        self.mysql_type = mysql_type
        self.nullable = nullable

    def to_sql(self) -> str:
        null_sql = 'NULL' if self.nullable else 'NOT NULL'
        return f"`{self.name}` {self.mysql_type} {null_sql}"


def map_layout_type_to_mysql(type_str: str, length: Optional[int], scale: Optional[int]) -> str:
    t = (type_str or '').strip().lower()
    # Normaliza tipos comuns do SIGTAP/Oracle/DBF
    if 'varchar2' in t or 'varchar' in t:
        if not length or length <= 0:
            length = 255
        if length > 65535:
            return 'TEXT'
        return f"VARCHAR({length})"
    if t == 'char' or 'char' in t:
        if not length or length <= 0:
            length = 1
        return f"CHAR({length})"
    if 'text' in t or 'clob' in t or 'memo' in t:
        return 'LONGTEXT' if (length and length > 65535) else 'TEXT'
    if 'number' in t or 'numeric' in t or 'decimal' in t:
        if length and (scale is None):
            scale = 0
        if length and scale is not None and length > 0:
            return f"DECIMAL({min(length,65)},{max(0,min(scale,30))})"
        return 'DECIMAL(38,0)'
    if 'bigint' in t:
        return 'BIGINT'
    if 'int' in t:
        return 'INT'
    if 'date' in t and 'time' not in t:
        return 'DATE'
    if 'timestamp' in t or 'datetime' in t or ('date' in t and 'time' in t):
        return 'DATETIME'
    if 'float' in t or 'double' in t:
        return 'DOUBLE'
    if 'bool' in t:
        return 'TINYINT(1)'
    return 'TEXT'


@dataclass
class TableLayout:
    columns: List[ColumnSpec]
    positions: List[dict]  # {'coluna': name, 'inicio': int, 'fim': int}


def parse_layout_dataframe(df: pd.DataFrame) -> TableLayout:
    """Força parser para layouts com colunas: coluna, tamanho, inicio, fim, tipo."""
    required = ['coluna', 'tamanho', 'inicio', 'fim', 'tipo']
    cols = [c.lower() for c in df.columns]
    if not all(r in cols for r in required):
        raise ValueError("Layout inválido: esperado colunas 'coluna', 'tamanho', 'inicio', 'fim', 'tipo'")
    specs: List[ColumnSpec] = []
    positions: List[dict] = []
    for _, row in df.iterrows():
        raw_name = str(row.get('coluna', '')).strip()
        if not raw_name:
            continue
        raw_type = str(row.get('tipo', '')).strip()
        try:
            length = int(str(row.get('tamanho', '')).strip())
        except Exception:
            length = None
        try:
            inicio = int(str(row.get('inicio', '')).strip())
            fim = int(str(row.get('fim', '')).strip())
        except Exception:
            inicio = None
            fim = None
        mysql_type = map_layout_type_to_mysql(raw_type, length, None)
        specs.append(ColumnSpec(raw_name, mysql_type, nullable=True))
        positions.append({'coluna': sanitize_table_name(raw_name), 'inicio': inicio, 'fim': fim})
    return TableLayout(columns=specs, positions=positions)
def read_fixedwidth_file(path: Path, layout_info: list, encoding: Optional[str]) -> pd.DataFrame:
    """Lê arquivo TXT de largura fixa conforme layout_info (lista de dicts com 'coluna', 'inicio', 'fim')."""
    # Detecta encoding se não especificado
    enc = encoding
    if enc is None:
        with open(path, 'rb') as fb:
            sample = fb.read(100_000)
        enc = detect_encoding(sample)

    records = []
    with open(path, 'r', encoding=enc, errors='replace') as f:
        for line in f:
            row = {}
            for col in layout_info:
                # INICIO e FIM são 1-based e inclusivos
                ini_1b = col.get('inicio') or 1
                fim_1b = col.get('fim')
                ini = max(0, int(ini_1b) - 1)
                if fim_1b is None:
                    # Se FIM ausente, tenta usar tamanho a partir das larguras médias
                    # Como não temos tamanho aqui, usa fatia até o final da linha
                    slice_txt = line[ini:]
                else:
                    fim = max(ini, int(fim_1b))
                    slice_txt = line[ini:fim]
                row[col['coluna']] = slice_txt.strip()
            records.append(row)
    return pd.DataFrame(records)


def load_layouts_from_files(files: List[Path], delimiter: Optional[str], encoding: Optional[str]) -> dict:
    """Procura *_layout.* e monta base_table -> List[ColumnSpec]."""
    layout_map = {}
    for p in files:
        stem = p.stem.lower()
        if stem.endswith('_layout') and p.suffix.lower() in {'.txt', '.csv'}:
            base = stem[:-7]
            try:
                df = read_tabular_file(p, delimiter=delimiter, encoding=encoding)
                layout = parse_layout_dataframe(df)
                layout_map[sanitize_table_name(base)] = layout
                logging.info(f"Layout detectado: {p.name} -> base {base} ({len(layout.columns)} colunas)")
            except Exception as e:
                logging.warning(f"Falha ao interpretar layout {p}: {e}")
    return layout_map


def ensure_table_from_layout(engine, table_name: str, specs: List[ColumnSpec], add_competencia: bool, recreate: bool = False) -> None:
    columns_sql = []
    seen = set()
    for s in specs:
        if not s.name or s.name in seen:
            continue
        seen.add(s.name)
        columns_sql.append(s.to_sql())
    if add_competencia and 'competencia' not in seen:
        columns_sql.insert(0, "`competencia` CHAR(6) NULL")

    ddl = f"CREATE TABLE IF NOT EXISTS `{table_name}` (" + ", ".join(columns_sql) + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    with engine.begin() as conn:
        if recreate:
            conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
        conn.execute(text(ddl))


def load_file_to_mysql(
    engine,
    file_path: Path,
    target_schema: Optional[str],
    delimiter: Optional[str],
    encoding: Optional[str],
    if_exists: str,
    competencia_override: Optional[str],
    chunksize: int,
    layout_specs: Optional[List[ColumnSpec]] = None,
    recreate_table_once: bool = False,
    force_table_name: Optional[str] = None,
) -> Tuple[str, int]:
    """Lê um arquivo e envia ao MySQL. Retorna (tabela, linhas)."""
    competencia = competencia_override or detect_competencia_from_path(file_path)

    # Determina nome base da tabela
    base = file_path.stem
    base = sanitize_table_name(base)
    # Se temos layout, usamos o nome base (sem sufixo de competência)
    if layout_specs is not None and force_table_name:
        table_name = sanitize_table_name(force_table_name)
    else:
        if competencia:
            table_name = sanitize_table_name(f"{base}_{competencia}")
        else:
            table_name = base

    logging.info(f"Lendo: {file_path.name} -> tabela {table_name}")

    ext = file_path.suffix.lower()
    if ext in {'.txt', '.csv'}:
        df = None
        if layout_specs is not None:
            layout_info = layout_specs.positions
            try:
                df = read_fixedwidth_file(file_path, layout_info, encoding=encoding)
            except Exception as e:
                logging.warning(f"Falha ao ler {file_path} como largura fixa: {e}")
                df = None
        if df is None:
            # fallback: ler normalmente
            df = read_tabular_file(file_path, delimiter=delimiter, encoding=encoding)
    elif ext == '.dbf':
        df = read_dbf_file(file_path, encoding=encoding)
    else:
        raise ValueError(f"Extensão não suportada: {ext}")

    # Adiciona coluna de competência se aplicável (apenas quando não há layout)
    if layout_specs is None and competencia and 'competencia' not in df.columns:
        df.insert(0, 'competencia', competencia)

    # Se houver layout: garantir DDL e alinhar colunas
    if layout_specs is not None:
        ensure_table_from_layout(engine, table_name, layout_specs.columns, add_competencia=False, recreate=recreate_table_once)
        # Ordena colunas conforme layout (mantendo apenas as conhecidas) e inclui faltantes como vazias
        layout_cols = [s.name for s in layout_specs.columns]
        final_cols = list(layout_cols)
        # Não adiciona 'competencia' artificial quando usamos layout
        # Garante todas as colunas presentes
        for c in final_cols:
            if c not in df.columns:
                df[c] = None
        df = df[[c for c in final_cols if c in df.columns]]

    # Envia para MySQL
    total_rows = int(df.shape[0])
    if total_rows == 0:
        logging.warning(f"Arquivo vazio: {file_path}")
        # Ainda cria tabela vazia se replace
        df.head(0).to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        return table_name, 0

    # Para cargas grandes, em chunks
    written = 0
    if total_rows > chunksize:
        for start in range(0, total_rows, chunksize):
            end = min(start + chunksize, total_rows)
            chunk = df.iloc[start:end]
            chunk.to_sql(table_name, con=engine, if_exists=('append' if start else if_exists), index=False, method='multi', chunksize=chunksize)
            written += len(chunk)
    else:
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False, method='multi', chunksize=chunksize)
        written = total_rows

    return table_name, written


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv()  # opcional
    parser = argparse.ArgumentParser(description='Carregar pastas/arquivos do SIGTAP para MySQL (banco: SIgtap por padrão).')
    parser.add_argument('--input', '-i', required=True, help='Caminho da pasta raiz de competências ou arquivo específico (TXT/CSV/ZIP/DBF).')
    parser.add_argument('--host', default=os.getenv('MYSQL_HOST', 'localhost'))
    parser.add_argument('--port', type=int, default=int(os.getenv('MYSQL_PORT', '3306')))
    parser.add_argument('--user', default=os.getenv('MYSQL_USER', 'root'))
    parser.add_argument('--password', default=os.getenv('MYSQL_PASSWORD', ''))
    parser.add_argument('--database', default=os.getenv('MYSQL_DATABASE', 'SIgtap'))
    parser.add_argument('--schema', default=None, help='Schema MySQL (opcional, normalmente não usado no MySQL).')
    parser.add_argument('--delimiter', default=None, help='Delimitador para TXT/CSV (se não informado, detecta).')
    parser.add_argument('--encoding', default=None, help='Codificação dos arquivos (se não informado, detecta).')
    parser.add_argument('--recreate', action='store_true', help='Recria as tabelas (if_exists=replace na primeira inserção de cada tabela).')
    parser.add_argument('--chunksize', type=int, default=25000, help='Tamanho do chunk para escrita no MySQL.')
    parser.add_argument('--verbose', '-v', action='store_true', help='Logs detalhados.')
    parser.add_argument('--dry-run', action='store_true', help='Lê arquivos e mostra planos sem inserir no banco.')

    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    root = Path(args.input).expanduser().resolve()
    if not root.exists():
        logging.error(f"Caminho não encontrado: {root}")
        return 2

    # Cria DB se necessário
    try:
        ensure_database(args.host, args.port, args.user, args.password, args.database)
    except Exception as e:
        logging.error(f"Falha ao garantir banco '{args.database}': {e}")
        return 3

    # Engine para o DB alvo
    engine = make_engine(args.host, args.port, args.user, args.password, args.database)

    # Descobre arquivos e trata ZIPs
    candidates: List[Path] = []

    if root.is_file():
        candidates = [root]
    else:
        candidates = list(iter_supported_files(root))

    if not candidates:
        logging.warning("Nenhum arquivo .txt .csv .zip .dbf encontrado.")
        return 0

    # Para arquivos ZIP, extrair e incluir seus conteúdos
    expanded_files: List[Path] = []
    temp_dirs: List[Path] = []
    try:
        for f in candidates:
            if f.suffix.lower() == '.zip':
                try:
                    tmpdir = extract_zip_to_temp(f)
                    temp_dirs.append(tmpdir)
                    for inner in iter_supported_files(tmpdir):
                        if inner.suffix.lower() != '.zip':  # evita zip aninhado
                            expanded_files.append(inner)
                except Exception as e:
                    logging.error(f"Falha ao extrair ZIP {f}: {e}")
            else:
                expanded_files.append(f)

        if not expanded_files:
            logging.warning("Nenhum arquivo processável após extrair ZIPs.")
            return 0

        # Carrega layouts primeiro
        layout_map = load_layouts_from_files(expanded_files, delimiter=args.delimiter, encoding=args.encoding)

        # Pre-cria todas as tabelas conforme layouts (sem coluna competencia extra)
        for base_key, layout in layout_map.items():
            try:
                ensure_table_from_layout(engine, base_key, layout.columns, add_competencia=False, recreate=args.recreate)
                logging.info(f"Tabela criada/atualizada a partir do layout: {base_key}")
            except Exception as e:
                logging.exception(f"Erro ao criar tabela '{base_key}' do layout: {e}")

        # Processa cada arquivo
        total_rows = 0
        tables_created = {}
        recreated_once = set()
        for fp in sorted(expanded_files):
            try:
                if args.dry_run:
                    comp = detect_competencia_from_path(fp)
                    tname = sanitize_table_name((fp.stem) + (f"_{comp}" if comp else ""))
                    logging.info(f"[DRY-RUN] {fp.name} -> tabela {tname}")
                    continue

                # Ignora arquivos de layout (apenas migrações)
                if fp.stem.lower().endswith('_layout'):
                    continue

                base_stem = strip_competencia_suffix(fp.stem)
                base_key = sanitize_table_name(base_stem)
                specs = layout_map.get(base_key)

                # Com layout, criamos a tabela tipada e sempre fazemos append.
                if_exists = 'append' if specs is not None else ('replace' if (args.recreate and sanitize_table_name(fp.stem) not in tables_created) else 'append')

                recreate_once = False
                if specs is not None:
                    recreate_once = False  # já fizemos a recriação antes

                table, written = load_file_to_mysql(
                    engine=engine,
                    file_path=fp,
                    target_schema=args.schema,
                    delimiter=args.delimiter,
                    encoding=args.encoding,
                    if_exists=if_exists,
                    competencia_override=detect_competencia_from_path(fp.parent),
                    chunksize=args.chunksize,
                    layout_specs=specs,
                    recreate_table_once=recreate_once,
                    force_table_name=base_key if specs is not None else None,
                )
                tables_created.setdefault(sanitize_table_name(fp.stem), True)
                total_rows += written
                logging.info(f"OK: {fp.name} -> {table} (+{written} linhas)")
            except Exception as e:
                logging.exception(f"ERRO ao processar {fp}: {e}")

        logging.info(f"Concluído. Linhas inseridas: {total_rows}")
        return 0
    finally:
        # Limpa temporários
        for d in temp_dirs:
            try:
                shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass


if __name__ == '__main__':
    sys.exit(main())
