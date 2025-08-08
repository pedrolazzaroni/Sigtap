<?php
// search.php: API para busca dinâmica no SIGTAP
header('Content-Type: application/json; charset=utf-8');

// Carrega variáveis do .env
function env($key, $default = null) {
    $lines = file_exists('.env') ? file('.env') : [];
    foreach ($lines as $line) {
        if (preg_match('/^' . preg_quote($key) . '=(.*)$/', trim($line), $m)) {
            return trim($m[1]);
        }
    }
    return $default;
}

$host = env('DB_HOST', '127.0.0.1');
$port = env('DB_PORT', '3306');
$db   = env('DB_DATABASE', 'sigtap');
$user = env('DB_USERNAME', 'root');
$pass = env('DB_PASSWORD', '');

$conn = new mysqli($host, $user, $pass, $db, (int)$port);
if ($conn->connect_error) {
    http_response_code(500);
    echo json_encode(['error' => 'Erro ao conectar ao banco: ' . $conn->connect_error]);
    exit;
}
$conn->set_charset('utf8mb4');

// Listar tabelas se não houver query
if (!isset($_GET['table'])) {
    $res = $conn->query("SHOW TABLES");
    $tables = [];
    while ($row = $res->fetch_array()) {
        $tables[] = $row[0];
    }
    echo json_encode(['tables' => $tables]);
    exit;
}

$table = preg_replace('/[^a-zA-Z0-9_]/', '', $_GET['table']);
$query = isset($_GET['query']) ? trim($_GET['query']) : '';
$page  = isset($_GET['page']) ? max(1, intval($_GET['page'])) : 1;
$perPage = 20;
$offset = ($page - 1) * $perPage;

// Descobre colunas

// Descobre colunas e tipos

// Tenta ler o layout correspondente (tb_nome_layout.txt ou .csv)
$layoutCols = [];
$layoutFile = null;
foreach (["txt", "csv"] as $ext) {
    $try = __DIR__ . "/{$table}_layout.$ext";
    if (file_exists($try)) {
        $layoutFile = $try;
        break;
    }
}
if ($layoutFile) {
    $f = fopen($layoutFile, 'r');
    $header = fgetcsv($f);
    $colIdx = array_search('Coluna', array_map('trim', $header));
    if ($colIdx === false) $colIdx = array_search('coluna', array_map('strtolower', $header));
    if ($colIdx !== false) {
        while (($row = fgetcsv($f)) !== false) {
            $col = trim($row[$colIdx]);
            if ($col) $layoutCols[] = $col;
        }
    }
    fclose($f);
}

$res = $conn->query("DESCRIBE `$table`");
$cols = [];
$textCols = [];
$allCols = [];
while ($row = $res->fetch_assoc()) {
    $colName = $row['Field'];
    $allCols[] = $colName;
    $type = strtolower($row['Type']);
    if (
        strpos($type, 'char') !== false ||
        strpos($type, 'text') !== false ||
        $type === 'varchar2' ||
        $type === 'char'
    ) {
        $textCols[] = $colName;
    }
}
// Se houver layout, usa a ordem e nomes do layout; senão, usa do banco
if (count($layoutCols) > 0) {
    $cols = array_values(array_filter($layoutCols, function($c) use ($allCols) {
        return in_array($c, $allCols);
    }));
} else {
    $cols = $allCols;
}

// Monta WHERE só para colunas texto
$where = '';
$params = [];
$types = '';
if ($query !== '' && count($textCols) > 0) {
    $like = '%' . $conn->real_escape_string($query) . '%';
    $w = [];
    foreach ($textCols as $col) {
        $w[] = "LOWER(`$col`) LIKE LOWER(?)";
        $params[] = $like;
        $types .= 's';
    }
    $where = 'WHERE ' . implode(' OR ', $w);
}

// Conta total
$sqlCount = "SELECT COUNT(*) FROM `$table` $where";
$stmt = $conn->prepare($sqlCount);
if ($where) $stmt->bind_param($types, ...$params);
$stmt->execute();
$stmt->bind_result($totalRows);
$stmt->fetch();
$stmt->close();

// Busca dados
$sql = "SELECT * FROM `$table` $where LIMIT $perPage OFFSET $offset";
$stmt = $conn->prepare($sql);
if ($where) $stmt->bind_param($types, ...$params);
$stmt->execute();
$res = $stmt->get_result();
$rows = [];
while ($row = $res->fetch_assoc()) {
    $rows[] = $row;
}
$stmt->close();

echo json_encode([
    'columns' => $cols,
    'rows' => $rows,
    'total' => $totalRows,
    'page' => $page,
    'perPage' => $perPage
]);
