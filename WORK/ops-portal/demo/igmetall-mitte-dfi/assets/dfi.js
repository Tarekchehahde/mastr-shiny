(function () {
  "use strict";

  var BASE = "/demo/igmetall-mitte-dfi";
  var RED = "#e30613";
  var DARK = "#1a1a1a";
  var GRAY = "#64748b";

  function formatDate(iso) {
    try {
      return new Date(iso + "T12:00:00").toLocaleDateString("de-DE", {
        day: "2-digit", month: "2-digit", year: "numeric"
      });
    } catch (e) {
      return iso;
    }
  }

  function renderKpis(kpis) {
    document.getElementById("kpi-handwerk").textContent = kpis.avg_handwerk_increase_pct.toFixed(1).replace(".", ",") + " %";
    document.getElementById("kpi-me").textContent = kpis.me_industry_2026_pct.toFixed(1).replace(".", ",") + " %";
    document.getElementById("kpi-count").textContent = String(kpis.agreements_count);
    document.getElementById("kpi-duration").textContent = String(kpis.avg_duration_months) + " Mon.";
  }

  function renderTable(rows) {
    var tbody = document.getElementById("tariff-body");
    tbody.innerHTML = rows.map(function (r) {
      return (
        "<tr class=\"border-b border-slate-100 hover:bg-red-50/40 transition-colors\" " +
        "data-search=\"" + (r.sector + " " + r.region + " " + r.type + " " + r.bonus).toLowerCase() + "\">" +
        "<td class=\"px-4 py-3 font-medium text-slate-800\">" + r.sector + "</td>" +
        "<td class=\"px-4 py-3 text-slate-600\">" + r.region + "</td>" +
        "<td class=\"px-4 py-3\"><span class=\"inline-flex px-2 py-0.5 rounded text-xs font-semibold " +
        (r.type === "Industrie" ? "bg-slate-800 text-white" : "bg-red-100 text-red-800") + "\">" + r.type + "</span></td>" +
        "<td class=\"px-4 py-3 text-slate-600 whitespace-nowrap\">" + formatDate(r.effective) + "</td>" +
        "<td class=\"px-4 py-3 font-bold text-red-700\">+" + r.increase_pct.toFixed(1).replace(".", ",") + " %</td>" +
        "<td class=\"px-4 py-3 text-slate-600 text-sm\">" + r.bonus + "</td>" +
        "<td class=\"px-4 py-3 text-slate-600\">" + r.duration_months + " Mon.</td></tr>"
      );
    }).join("");
    bindSearch();
  }

  function bindSearch() {
    var input = document.getElementById("table-search");
    var count = document.getElementById("result-count");
    if (!input) return;

    function filter() {
      var q = input.value.trim().toLowerCase();
      var visible = 0;
      document.querySelectorAll("#tariff-body tr").forEach(function (row) {
        var show = !q || row.getAttribute("data-search").indexOf(q) !== -1;
        row.style.display = show ? "" : "none";
        if (show) visible++;
      });
      if (count) count.textContent = visible + " Einträge";
    }

    input.oninput = filter;
    filter();
  }

  function renderCharts(data) {
    var tooltip = {
      backgroundColor: DARK,
      titleColor: "#fff",
      bodyColor: "#cbd5e1",
      borderColor: RED,
      borderWidth: 1,
      padding: 10
    };

    new Chart(document.getElementById("sectorChart"), {
      type: "bar",
      data: {
        labels: data.sector_comparison.labels,
        datasets: [{
          label: "Tariferhöhung (%)",
          data: data.sector_comparison.values,
          backgroundColor: [RED, "#b0050f", "#dc2626", "#991b1b", "#7f1d1d"],
          borderRadius: 6,
          borderSkipped: false
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            ...tooltip,
            callbacks: { label: function (ctx) { return " +" + ctx.parsed.y.toFixed(1).replace(".", ",") + " %"; } }
          }
        },
        scales: {
          x: { grid: { display: false }, ticks: { font: { size: 11 }, maxRotation: 45, minRotation: 0 } },
          y: {
            grid: { color: "#f1f5f9" },
            title: { display: true, text: "Erhöhung in %", color: GRAY, font: { size: 11 } },
            ticks: { callback: function (v) { return v + " %"; } }
          }
        }
      }
    });

    new Chart(document.getElementById("durationChart"), {
      type: "line",
      data: {
        labels: data.duration_distribution.labels,
        datasets: [{
          label: "Anzahl Abschlüsse",
          data: data.duration_distribution.values,
          borderColor: RED,
          backgroundColor: "rgba(227,6,19,0.08)",
          pointBackgroundColor: RED,
          pointRadius: 5,
          pointHoverRadius: 7,
          fill: true,
          tension: 0.3
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            ...tooltip,
            callbacks: { label: function (ctx) { return " " + ctx.parsed.y + " Abschlüsse"; } }
          }
        },
        scales: {
          x: {
            grid: { color: "#f1f5f9" },
            title: { display: true, text: "Vertragslaufzeit", color: GRAY, font: { size: 11 } }
          },
          y: {
            grid: { color: "#f1f5f9" },
            title: { display: true, text: "Anzahl", color: GRAY, font: { size: 11 } },
            beginAtZero: true,
            ticks: { stepSize: 1 }
          }
        }
      }
    });
  }

  fetch(BASE + "/assets/dfi-data.json")
    .then(function (r) { return r.json(); })
    .then(function (data) {
      renderKpis(data.kpis);
      renderTable(data.agreements);
      renderCharts(data);
      var note = document.getElementById("source-note");
      if (note) note.textContent = data.meta.source_note + " · Stand " + data.meta.updated;
    })
    .catch(function (err) {
      console.error(err);
      var el = document.getElementById("load-error");
      if (el) el.hidden = false;
    });
})();
