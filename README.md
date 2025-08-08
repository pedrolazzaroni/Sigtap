# Loader SIGTAP -> MySQL (SIgtap)

Este script lê uma pasta com competências baixadas do SIGTAP (TXT/CSV/ZIP/DBF) e insere no MySQL no banco `SIgtap` (ou outro que você informar).

## Pré-requisitos
- Python 3.9+
- MySQL acessível (localhost por padrão)
- Pacotes Python:

```bash
pip install -r requirements.txt
```

Opcional: crie um arquivo `.env` com as variáveis padrão:

```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DATABASE=SIgtap
```

## Como usar

- Para carregar todos os arquivos suportados dentro de uma pasta de competências (inclui ZIPs):

```powershell
python .\load_sigtap.py --input "C:\\caminho\\para\\pasta_competencias"
```

- Para um arquivo específico:

```powershell
python .\load_sigtap.py --input "C:\\caminho\\para\\arquivo.txt"
```

- Parâmetros úteis:
  - `--database` Nome do banco (padrão: SIgtap). Será criado se não existir.
  - `--user --password --host --port` Credenciais do MySQL.
  - `--recreate` Recria as tabelas na primeira inserção (drop/replace) por arquivo-base.
  - `--delimiter` e `--encoding` caso queira forçar.
  - `--chunksize` tamanho do batch para escrita (padrão 25000).
  - `--dry-run` Apenas mostra o plano sem inserir.
  - `--verbose` Logs detalhados.

## Nome das tabelas e competência

- O nome-base da tabela é derivado do nome do arquivo (ex.: `tb_procedimento`), normalizado.
- Se uma competência `AAAAMM` for detectada no nome do arquivo ou da pasta, será adicionado sufixo `_AAAAMM` e também uma coluna `competencia` na tabela.

## Formatos suportados
- TXT/CSV delimitados por `| ; , \t` (detecta automaticamente)
- DBF (requer `dbfread`)
- ZIP (será extraído para temporário e seus TXT/CSV/DBF processados)

## Exemplos

```powershell
# Carregar todos os arquivos da pasta
python .\load_sigtap.py -i "D:\\SIGTAP\\202501"

# Dry-run com logs
python .\load_sigtap.py -i "D:\\SIGTAP" --dry-run -v

# Forçando delimitador e encoding
python .\load_sigtap.py -i "D:\\SIGTAP" --delimiter "|" --encoding latin-1

# Outro banco/host
python .\load_sigtap.py -i "D:\\SIGTAP" --database SIgtap --host 127.0.0.1 --user root --password "minhasenha"
```

## Observações
- O MySQL será configurado com charset `utf8mb4`.
- Na primeira escrita por arquivo-base com `--recreate`, a estratégia é `replace` e, nas seguintes, `append`.
- Colunas são mantidas como texto (`dtype=str`) para evitar perdas; converta depois com SQL se necessário.
- Se aparecer erro de acesso ao MySQL, verifique host/porta/usuário e permissões de criação de banco.
