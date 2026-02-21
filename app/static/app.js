// 株売買支援システム - フロントエンドスクリプト

document.addEventListener('DOMContentLoaded', () => {
  // ツールチップ初期化
  document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => new bootstrap.Tooltip(el));

  // アクティブなナビリンクの強調（buy/sell のみ、hold はどちらもハイライトしない）
  const path = location.pathname;
  if (path === '/') {
    document.getElementById('nav-home')?.classList.add('active');
  } else if (path.startsWith('/candidates')) {
    const signal = new URLSearchParams(location.search).get('signal');
    if (signal === 'sell') document.getElementById('nav-sell')?.classList.add('active');
    if (signal === 'buy')  document.getElementById('nav-buy')?.classList.add('active');
  }

  // ソート可能テーブル（クリック＋キーボード対応）
  document.querySelectorAll('.sortable-table th.sortable').forEach(th => {
    th.addEventListener('click', () => sortByColumn(th));
    th.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        sortByColumn(th);
      }
    });
  });
});

function sortByColumn(th) {
  const table = th.closest('table');
  const tbody = table.querySelector('tbody');
  const col   = parseInt(th.dataset.col, 10);
  const asc   = th.dataset.order !== 'asc';

  // ソート方向をトグル・aria-sort 更新
  table.querySelectorAll('th.sortable').forEach(h => {
    h.dataset.order = '';
    h.classList.remove('sort-asc', 'sort-desc');
    h.setAttribute('aria-sort', 'none');
  });
  th.dataset.order = asc ? 'asc' : 'desc';
  th.classList.add(asc ? 'sort-asc' : 'sort-desc');
  th.setAttribute('aria-sort', asc ? 'ascending' : 'descending');

  const rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort((a, b) => {
    const cellA = a.cells[col];
    const cellB = b.cells[col];
    const rawA = cellA?.dataset.value ?? cellA?.textContent.trim() ?? '';
    const rawB = cellB?.dataset.value ?? cellB?.textContent.trim() ?? '';
    const numA = parseFloat(rawA);
    const numB = parseFloat(rawB);
    if (!isNaN(numA) && !isNaN(numB)) {
      return asc ? numA - numB : numB - numA;
    }
    return asc
      ? rawA.localeCompare(rawB, 'ja')
      : rawB.localeCompare(rawA, 'ja');
  });

  rows.forEach(r => tbody.appendChild(r));
}
