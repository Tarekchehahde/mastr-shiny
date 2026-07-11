(function () {
  "use strict";

  var RED = "#e30613";
  var GRAY = "#777777";

  var PALETTE = [
    "#e30613", "#b0050f", "#ff3340", "#ff8899",
    "#555555", "#888888", "#bbbbbb"
  ];

  var tooltipDefaults = {
    backgroundColor: "#1A1A1A",
    titleColor: "#FFFFFF",
    bodyColor: "#CCCCCC",
    borderColor: RED,
    borderWidth: 1,
    padding: 10,
    cornerRadius: 4
  };

  Chart.defaults.font.family = "'Inter', system-ui, sans-serif";
  Chart.defaults.color = "#444";

  new Chart(document.getElementById("ageChart"), {
    type: "bar",
    data: {
      labels: ["18–24", "25–29", "30–34", "35–39", "40–44", "45–49", "50–54", "55–59", "60+"],
      datasets: [{
        label: "Mitarbeiter",
        data: [18, 27, 38, 49, 62, 50, 46, 48, 32],
        backgroundColor: [
          "#FFAABB", "#FF8899", "#FF5566",
          RED, RED,
          "#B5001A", "#8B0015", "#6A0010", "#440011"
        ],
        borderRadius: 4,
        borderSkipped: false
      }]
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          ...tooltipDefaults,
          callbacks: { label: function (ctx) { return " " + ctx.parsed.x + " Mitarbeiter"; } }
        }
      },
      scales: {
        x: {
          grid: { color: "#F0F0F0" },
          title: { display: true, text: "Anzahl Mitarbeiter", color: GRAY, font: { size: 11 } }
        },
        y: { grid: { display: false } }
      }
    }
  });

  new Chart(document.getElementById("positionChart"), {
    type: "doughnut",
    data: {
      labels: [
        "Produktionsarbeiter", "Ingenieure", "Techniker",
        "Führungskräfte", "Personal & Verwaltung", "Vertrieb & Marketing", "IT"
      ],
      datasets: [{
        data: [145, 72, 58, 25, 31, 22, 17],
        backgroundColor: PALETTE,
        borderWidth: 2,
        borderColor: "#FFFFFF",
        hoverOffset: 8
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: "60%",
      plugins: {
        legend: {
          position: "right",
          labels: { boxWidth: 11, padding: 10, font: { size: 10 } }
        },
        tooltip: {
          ...tooltipDefaults,
          callbacks: {
            label: function (ctx) {
              var total = ctx.dataset.data.reduce(function (a, b) { return a + b; }, 0);
              var pct = ((ctx.parsed / total) * 100).toFixed(1);
              return " " + ctx.label + ": " + ctx.parsed + " (" + pct + " %)";
            }
          }
        }
      }
    }
  });

  new Chart(document.getElementById("tenureChart"), {
    type: "line",
    data: {
      labels: ["< 1 J.", "1–2 J.", "3–5 J.", "6–10 J.", "11–15 J.", "16–20 J.", "21–25 J.", "26–30 J.", "30+ J."],
      datasets: [{
        label: "Mitarbeiter",
        data: [14, 22, 41, 67, 52, 48, 38, 55, 33],
        borderColor: RED,
        backgroundColor: "rgba(227,6,19,0.10)",
        pointBackgroundColor: RED,
        pointRadius: 5,
        pointHoverRadius: 7,
        fill: true,
        tension: 0.35
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          ...tooltipDefaults,
          callbacks: { label: function (ctx) { return " " + ctx.parsed.y + " Mitarbeiter"; } }
        }
      },
      scales: {
        x: {
          grid: { color: "#F0F0F0" },
          title: { display: true, text: "Jahre im Unternehmen", color: GRAY, font: { size: 11 } },
          ticks: { font: { size: 10 } }
        },
        y: {
          grid: { color: "#F0F0F0" },
          title: { display: true, text: "Mitarbeiter", color: GRAY, font: { size: 11 } }
        }
      }
    }
  });

  new Chart(document.getElementById("genderChart"), {
    type: "bar",
    data: {
      labels: ["Produktion", "Ingenieure", "Techniker", "Führung", "Personal", "Vertrieb", "IT"],
      datasets: [
        {
          label: "Männlich",
          data: [110, 52, 37, 16, 10, 12, 11],
          backgroundColor: RED,
          borderRadius: 3
        },
        {
          label: "Weiblich",
          data: [35, 20, 21, 9, 21, 10, 6],
          backgroundColor: "#BBBBBB",
          borderRadius: 3
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "top",
          labels: { boxWidth: 11, padding: 14, font: { size: 11 } }
        },
        tooltip: { ...tooltipDefaults }
      },
      scales: {
        x: { grid: { display: false }, ticks: { font: { size: 10 } } },
        y: {
          grid: { color: "#F0F0F0" },
          title: { display: true, text: "Mitarbeiter", color: GRAY, font: { size: 11 } }
        }
      }
    }
  });
})();
