document.addEventListener('DOMContentLoaded', function() {
    const tableSel = document.getElementById('table');
    const form = document.getElementById('searchForm');
    const resultsDiv = document.getElementById('results');
    let currentPage = 1;

    // Carrega tabelas disponíveis
    fetch('search.php')
        .then(r => r.json())
        .then(data => {
            tableSel.innerHTML = '';
            data.tables.forEach(t => {
                const opt = document.createElement('option');
                opt.value = t;
                opt.textContent = t;
                tableSel.appendChild(opt);
            });
        });

    function renderResults(data) {
        if (!data || !data.rows || data.rows.length === 0) {
            resultsDiv.innerHTML = '<p>Nenhum resultado encontrado.</p>';
            return;
        }
        // Garante que a ordem dos dados siga a ordem das colunas retornadas (layout)
        let html = '<div class="table-wrap"><table><thead><tr>';
        data.columns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        html += '</tr></thead><tbody>';
        data.rows.forEach(row => {
            html += '<tr>';
            data.columns.forEach(col => {
                html += `<td>${row[col] !== undefined ? row[col] : ''}</td>`;
            });
            html += '</tr>';
        });
        html += '</tbody></table></div>';
        // Paginação
        const totalPages = Math.ceil(data.total / data.perPage);
        if (totalPages > 1) {
            html += '<div class="pagination">';
            for (let i = 1; i <= totalPages; i++) {
                html += `<button class="${i === data.page ? 'active' : ''}" data-page="${i}">${i}</button>`;
            }
            html += '</div>';
        }
        resultsDiv.innerHTML = html;
        // Eventos de paginação
        document.querySelectorAll('.pagination button').forEach(btn => {
            btn.onclick = function() {
                currentPage = parseInt(this.dataset.page);
                doSearch();
            };
        });
    }

    function doSearch() {
        const table = tableSel.value;
        const query = document.getElementById('query').value.trim();
        fetch(`search.php?table=${encodeURIComponent(table)}&query=${encodeURIComponent(query)}&page=${currentPage}`)
            .then(r => r.json())
            .then(renderResults)
            .catch(() => {
                resultsDiv.innerHTML = '<p>Erro ao buscar dados.</p>';
            });
    }

    form.onsubmit = function() {
        currentPage = 1;
        doSearch();
    };
    tableSel.onchange = function() {
        currentPage = 1;
        doSearch();
    };

    // Busca inicial ao carregar
    setTimeout(doSearch, 400);
});
