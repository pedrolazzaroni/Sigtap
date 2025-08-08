# Interface de Pesquisa SIGTAP (PHP)

## Como usar

1. Copie os arquivos `index.php`, `search.php`, `style.css` e `script.js` para a pasta onde está seu banco SIGTAP.
2. Certifique-se de que o arquivo `.env` está preenchido com as credenciais do banco (já criado anteriormente).
3. Acesse `http://localhost/SIGTAP/index.php` no navegador.

## Recursos
- Pesquisa em qualquer tabela SIGTAP importada.
- Busca por qualquer termo em todas as colunas.
- Resultados em tabela responsiva, com paginação.
- Interface moderna, rápida e intuitiva.

## Segurança
- As consultas usam prepared statements para evitar SQL Injection.
- Apenas tabelas e colunas válidas são acessíveis.

## Personalização
- Edite `style.css` para mudar o visual.
- O backend pode ser adaptado para filtros avançados, exportação, etc.

## Observação
- O sistema lista automaticamente todas as tabelas do banco configurado.
- Se não aparecerem tabelas, verifique se a importação SIGTAP foi concluída e se o banco está acessível.
