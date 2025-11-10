document.addEventListener("DOMContentLoaded", async () => {
  console.log("✅ dashboard.js chargé avec succès");

  try {
    const response = await fetch("/api/stats");
    const data = await response.json();

    const labels = data.labels || [];

    // --- Fonction pour créer un graphique en barres empilées ---
    const ctxBar = document.getElementById("bars_centered");
    if (ctxBar) {
      new Chart(ctxBar.getContext("2d"), {
        type: "bar",
        data: {
          labels: labels,
          datasets: [
            { label: "Lu", data: data.lus, backgroundColor: "#28a745" },
            { label: "Non lu", data: data.non_lus, backgroundColor: "#dc3545" }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { position: "top" },
            datalabels: {
              color: "#fff",
              font: { weight: "bold", size: 12 },
              anchor: "center",
              align: "center"
            }
          },
          scales: {
            x: { stacked: true },
            y: { stacked: true, beginAtZero: true }
          }
        },
        plugins: [ChartDataLabels]
      });
    }

    // --- Fonction pour créer des graphiques circulaires (pie) ---
    const makePie = (id, arr, colors) => {
      const canvas = document.getElementById(id);
      if (!canvas) return;
      new Chart(canvas.getContext("2d"), {
        type: "pie",
        data: {
          labels: labels,
          datasets: [
            {
              data: arr,
              backgroundColor:
                colors ||
                ["#0d6efd", "#198754", "#ffc107", "#0dcaf0", "#dc3545", "#6c757d"]
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: true,
          plugins: {
            legend: { display: false },
            datalabels: {
              color: "#fff",
              font: { weight: "bold", size: 14 },
              formatter: (v) => (v > 0 ? v : "")
            }
          }
        },
        plugins: [ChartDataLabels]
      });
    };

    makePie("pie_totals", data.totals);
    makePie("pie_views", data.views);
    makePie("pie_downloads", data.downloads);

  } catch (err) {
    console.error("❌ Erreur lors du chargement des stats :", err);
  }
});
