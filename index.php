<!DOCTYPE html>
<html lang="pt-br">
<head>
    <meta charset="UTF-8">
    <title>Pesquisa SIGTAP</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <div class="container">
        <h1>Pesquisa</h1>
        <form id="searchForm" onsubmit="return false;">
            <label for="table">Tabela:</label>
            <select id="table" name="table"></select>
            <label for="query">Buscar por:</label>
            <input type="text" id="query" name="query" placeholder="Digite o termo..." autocomplete="off">
            <button type="submit">Pesquisar</button>
        </form>
        <div id="results"></div>
    </div>
    <script src="script.js"></script>
</body>
</html>
