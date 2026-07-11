(function () {
  "use strict";

  var BASE = "/demo/igmetall-mitte-factory";
  var DATA_URL = BASE + "/assets/demo-data.json";

  function $(sel, root) {
    return (root || document).querySelector(sel);
  }

  function formatNum(n) {
    return Number(n).toLocaleString("de-DE");
  }

  function pct(members, total) {
    if (!total) return 0;
    return Math.round((members / total) * 100);
  }

  function loadData() {
    return fetch(DATA_URL).then(function (r) {
      if (!r.ok) throw new Error("data load failed");
      return r.json();
    });
  }

  function statCell(val, lbl, cls) {
    return (
      '<div class="stat"><div class="val ' + (cls || "") + '">' + val +
      '</div><div class="lbl">' + lbl + "</div></div>"
    );
  }

  function renderStats(data) {
    var s = data.summary;
    var el = $("#igm-stats");
    if (!el) return;
    el.innerHTML =
      statCell(s.buildings, "Gebäude") +
      statCell(formatNum(s.employees_total), "Beschäftigte gesamt", "red") +
      statCell(formatNum(s.ig_metall_members), "IG Metall Mitglieder", "green") +
      statCell(formatNum(s.not_yet_members), "Noch nicht organisiert", "amber") +
      statCell(s.organizing_rate_pct + "%", "Organisationsgrad");
  }

  function tooltipHtml(b) {
    var rate = pct(b.ig_metall_members, b.employees_total);
    return (
      '<div class="marker-tooltip" role="tooltip">' +
      '<p class="tooltip-title">' + b.name + "</p>" +
      '<p class="tooltip-dept">' + b.department + " · Demo</p>" +
      '<div class="tooltip-row"><span class="k">Beschäftigte gesamt</span><span class="v total">' +
      formatNum(b.employees_total) + "</span></div>" +
      '<div class="tooltip-row"><span class="k">IG Metall Mitglieder</span><span class="v member">' +
      formatNum(b.ig_metall_members) + "</span></div>" +
      '<div class="tooltip-row"><span class="k">Noch nicht organisiert</span><span class="v pending">' +
      formatNum(b.not_yet_members) + "</span></div>" +
      '<div class="tooltip-row"><span class="k">Organisationsgrad</span><span class="v">' +
      rate + "%</span></div></div>"
    );
  }

  function renderMap(data) {
    var mapEl = $("#site-map");
    if (!mapEl) return;

    var markersHtml = data.buildings.map(function (b) {
      var short = b.short_label || b.name.split(" — ")[0];
      var num = b.index || "";
      return (
        '<button type="button" class="building-marker" data-id="' + b.id + '" ' +
        'style="left:' + b.x_pct + "%;top:" + b.y_pct + '%" ' +
        'aria-label="' + b.name + ': ' + formatNum(b.employees_total) + ' Beschäftigte">' +
        '<span class="marker-pin"><span class="marker-number">' + num + "</span></span>" +
        '<span class="marker-label">' + short + "</span>" +
        '<span class="marker-hint">Hover für Details</span>' +
        tooltipHtml(b) +
        "</button>"
      );
    }).join("");

    mapEl.innerHTML =
      '<img src="' + BASE + '/assets/factory-site.png" alt="Isometrische Werksansicht (Demo)" loading="lazy"/>' +
      '<div class="markers">' + markersHtml + "</div>";

    bindMarkerInteractions(data);
  }

  function renderBuildingList(data) {
    var list = $("#building-list");
    if (!list) return;

    list.innerHTML = data.buildings.map(function (b) {
      var num = b.index ? '<span class="list-num">' + b.index + "</span> " : "";
      return (
        '<li data-id="' + b.id + '">' +
        "<div><div class=\"name\">" + num + b.name + "</div>" +
        '<div class="dept">' + b.department + "</div></div>" +
        '<div class="counts">' + formatNum(b.employees_total) + " gesamt<br>" +
        '<span class="member">' + formatNum(b.ig_metall_members) + " IG Metall</span> · " +
        '<span class="pending">' + formatNum(b.not_yet_members) + " offen</span></div></li>"
      );
    }).join("");

    list.querySelectorAll("li").forEach(function (li) {
      li.addEventListener("click", function () {
        highlightBuilding(li.getAttribute("data-id"));
      });
    });
  }

  function highlightBuilding(id) {
    document.querySelectorAll(".building-marker, .building-list li").forEach(function (el) {
      el.classList.toggle("is-active", el.getAttribute("data-id") === id);
    });
    if (id) {
      var marker = document.querySelector('.building-marker[data-id="' + id + '"]');
      if (marker) {
        marker.scrollIntoView({ behavior: "smooth", block: "center", inline: "center" });
      }
    }
  }

  function bindMarkerInteractions(data) {
    var activeId = null;

    document.querySelectorAll(".building-marker").forEach(function (marker) {
      marker.addEventListener("click", function (e) {
        e.stopPropagation();
        var id = marker.getAttribute("data-id");
        if (activeId === id) {
          activeId = null;
          highlightBuilding("");
        } else {
          activeId = id;
          highlightBuilding(id);
        }
      });
    });

    document.addEventListener("click", function () {
      activeId = null;
      highlightBuilding("");
    });

    var map = $("#site-map");
    if (map) {
      map.addEventListener("click", function (e) {
        e.stopPropagation();
      });
    }
  }

  function setMeta(data) {
    var disclaimer = $("#disclaimer-text");
    if (disclaimer && data.meta) {
      disclaimer.innerHTML =
        "<strong>Rechtlicher Hinweis:</strong> " + data.meta.disclaimer +
        " · Stand " + data.meta.updated;
    }

    var credit = $("#image-credit");
    if (credit && data.meta) {
      credit.textContent = data.meta.image_credit;
    }
  }

  window.IGMetallFactory = {
    init: function () {
      loadData()
        .then(function (data) {
          setMeta(data);
          renderStats(data);
          renderMap(data);
          renderBuildingList(data);
        })
        .catch(function (err) {
          console.error(err);
          var main = $(".wrap");
          if (main) {
            main.insertAdjacentHTML(
              "afterbegin",
              '<div class="disclaimer">Daten konnten nicht geladen werden. Bitte Seite neu laden.</div>'
            );
          }
        });
    }
  };
})();
