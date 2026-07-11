(function () {
  "use strict";

  var BASE = "/demo/ccs-ops-pulse";
  var DATA_URL = BASE + "/assets/demo-data.json";

  var NAV = [
    { id: "heute", href: BASE + "/", label: "Heute" },
    { id: "kantinen", href: BASE + "/kantinen/", label: "Kantinen" },
    { id: "events", href: BASE + "/events/", label: "Events" },
    { id: "nachhaltigkeit", href: BASE + "/nachhaltigkeit/", label: "Nachhaltigkeit" },
    { id: "qualitaet", href: BASE + "/qualitaet/", label: "Qualität" }
  ];

  function $(sel, root) {
    return (root || document).querySelector(sel);
  }

  function chipClass(status) {
    if (status === "ok") return "chip chip-ok";
    if (status === "event") return "chip chip-event";
    return "chip chip-hint";
  }

  function formatDate(iso) {
    try {
      return new Date(iso + "T12:00:00").toLocaleDateString("de-DE", {
        weekday: "short",
        day: "2-digit",
        month: "short",
        year: "numeric"
      });
    } catch (e) {
      return iso;
    }
  }

  function renderNav(activeId) {
    var el = $("#ccs-nav");
    if (!el) return;
    el.innerHTML = NAV.map(function (item) {
      var cls = item.id === activeId ? " active" : "";
      return '<a href="' + item.href + '" class="' + cls.trim() + '">' + item.label + "</a>";
    }).join("");
  }

  function loadData() {
    return fetch(DATA_URL).then(function (r) {
      if (!r.ok) throw new Error("data load failed");
      return r.json();
    });
  }

  function initMap(sites) {
    var mapEl = $("#map");
    if (!mapEl || typeof L === "undefined") return;

    var map = L.map("map", { scrollWheelZoom: false }).setView([50.973, 11.02], 12);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap",
      maxZoom: 18
    }).addTo(map);

    sites.forEach(function (site) {
      var color = site.status === "event" ? "#60a5fa" : site.status === "hint" ? "#fbbf24" : "#34d399";
      var marker = L.circleMarker([site.lat, site.lng], {
        radius: 9,
        fillColor: color,
        color: "#0c1210",
        weight: 2,
        fillOpacity: 0.9
      }).addTo(map);
      marker.bindPopup(
        "<strong>" + site.name + "</strong><br>" +
        site.address + "<br><em>" + site.status_label + "</em>"
      );
    });

    setTimeout(function () { map.invalidateSize(); }, 200);
  }

  function renderSiteList(sites, container) {
    if (!container) return;
    container.innerHTML = sites.map(function (s) {
      return (
        '<li><span class="' + chipClass(s.status) + '">' + s.status_label + "</span>" +
        "<div><strong>" + s.name + "</strong><br>" +
        '<span style="font-size:0.8rem;color:var(--muted)">' + s.address + "</span><br>" +
        '<span style="font-size:0.75rem">' + s.note + " · Demo</span></div></li>"
      );
    }).join("");
  }

  function renderHeute(data) {
    var stats = $("#ccs-stats");
    if (stats) {
      stats.innerHTML =
        statCell(data.summary.sites_total, "Standorte") +
        statCell(data.summary.event_areas, "Event-Bereiche") +
        statCell(data.summary.staff_approx, "Mitarbeiter ca.") +
        statCell(data.summary.events_this_week, "Events diese Woche");
    }

    var today = new Date().toISOString().slice(0, 10);
    var banner = $("#event-banner");
    if (banner) {
      var todayEvents = data.events.filter(function (e) { return e.date === today; });
      var next = data.events.filter(function (e) { return e.date >= today; }).slice(0, 1)[0];
      var show = todayEvents[0] || next;
      if (show) {
        var prefix = todayEvents.length ? "Heute" : "Nächstes Event";
        banner.innerHTML =
          '<div class="banner-event"><strong>' + prefix + ":</strong> " +
          show.title + " · " + show.venue + " · ~" +
          show.guests_est.toLocaleString("de-DE") + " Gäste <span style=\"opacity:0.7\">(Demo)</span></div>";
        banner.hidden = false;
      } else {
        banner.hidden = true;
      }
    }

    initMap(data.sites);
    renderSiteList(data.sites, $("#site-list"));
  }

  function statCell(val, lbl) {
    return '<div class="stat"><div class="val">' + val + '</div><div class="lbl">' + lbl + "</div></div>";
  }

  function mealTags(tags) {
    return tags.map(function (t) {
      var cls = t.indexOf("bio") >= 0 ? "tag tag-bio" : "tag";
      var label = t.replace(/_/g, " ");
      return '<span class="' + cls + '">' + label + "</span>";
    }).join("");
  }

  function renderKantinen(data) {
    var select = $("#kantine-select");
    var detail = $("#kantine-detail");
    if (!select || !detail) return;

    select.innerHTML = data.kantinen.map(function (k, i) {
      return '<option value="' + i + '">' + k.name + "</option>";
    }).join("");

    function show(idx) {
      var k = data.kantinen[idx];
      detail.innerHTML =
        '<p class="panel-note">' + k.hours + " · Demo-Werte</p>" +
        "<h3>Speisen heute</h3><ul style=\"padding-left:1.1rem;margin:0 0 1rem\">" +
        k.meals_today.map(function (m) {
          return "<li style=\"margin-bottom:0.5rem\"><strong>" + m.name + "</strong><br>" + mealTags(m.tags) + "</li>";
        }).join("") +
        "</ul>" +
        "<p><strong>Auslastung Mittagspeak</strong></p>" +
        '<div class="bar-wrap"><div class="bar-fill" style="width:' + k.occupancy_pct + '%"></div></div>' +
        "<p style=\"font-size:0.85rem;color:var(--muted)\">" + k.occupancy_pct + "% · Demo</p>" +
        "<p><strong>Bio-Anteil Speiseplan</strong> " + k.bio_share_pct + "%</p>" +
        "<p><strong>HACCP Kontrollen</strong> " + k.haccp_checks_done + " / " + k.haccp_checks_total + " erledigt</p>";
    }

    select.addEventListener("change", function () { show(Number(select.value)); });
    show(0);
  }

  function renderEvents(data) {
    var list = $("#events-list");
    if (!list) return;
    var today = new Date().toISOString().slice(0, 10);
    list.innerHTML = data.events.map(function (e) {
      var staffCls = e.staff_need === "high" ? "chip chip-event" : "chip chip-hint";
      return (
        '<div class="event-row">' +
        '<div class="date">' + formatDate(e.date) + (e.date === today ? " · HEUTE" : "") + "</div>" +
        "<h3>" + e.title + "</h3>" +
        '<div class="event-meta">' +
        "<span>📍 " + e.venue + "</span>" +
        "<span>👥 ~" + e.guests_est.toLocaleString("de-DE") + " Gäste (Demo)</span>" +
        "<span>🍽 " + e.mode + "</span>" +
        '<span class="' + staffCls + '">Personal ' + e.staff_label + "</span>" +
        "</div></div>"
      );
    }).join("");
  }

  function renderNachhaltigkeit(data) {
    var n = data.nachhaltigkeit;
    var root = $("#nachhaltigkeit-content");
    if (!root) return;
    root.innerHTML =
      '<div class="kpi-grid">' +
      kpi(n.bio_share_pct + "%", "Bio-Anteil Menüplanung") +
      kpi(n.regional_suppliers, "Regionale Lieferanten") +
      kpi(n.food_waste_kg_week + " kg", "Lebensmittelverluste / Woche") +
      kpi(n.strompreis_eur_mwh + " €", "Strom Day-ahead (Demo)") +
      kpi(n.pv_potential_kwp_erfurt.toLocaleString("de-DE") + " kWp", "PV-Potenzial Region") +
      "</div>" +
      '<p style="margin-top:1rem;font-size:0.88rem">' + n.strompreis_hint + "</p>" +
      '<p style="font-size:0.78rem;color:var(--muted)">' + n.pv_note + "</p>" +
      '<div class="btn-row">' +
      '<a class="btn btn-primary" href="' + data.links.strom_live + '">Live Strompreise (Hub)</a>' +
      '<a class="btn" href="' + data.links.ccs_public + '" target="_blank" rel="noopener">CCS Website</a>' +
      "</div>";
  }

  function kpi(num, cap) {
    return '<div class="kpi"><div class="num">' + num + '</div><div class="cap">' + cap + " · Demo</div></div>";
  }

  function renderQualitaet(data) {
    var table = $("#haccp-table");
    if (!table) return;
    var rows = data.haccp.map(function (r) {
      function cell(v) {
        var cls = v === "ok" ? "dot-ok" : "dot-pending";
        return '<span class="status-dot ' + cls + '" title="' + v + '"></span>';
      }
      return (
        "<tr><td>" + r.site + "</td>" +
        "<td>" + cell(r.temp) + "</td>" +
        "<td>" + cell(r.cleaning) + "</td>" +
        "<td>" + cell(r.delivery) + "</td>" +
        "<td>" + cell(r.docs) + "</td></tr>"
      );
    }).join("");
    table.innerHTML =
      "<thead><tr><th>Standort</th><th>Temp.</th><th>Reinigung</th><th>Lieferung</th><th>Doku</th></tr></thead>" +
      "<tbody>" + rows + "</tbody>";
  }

  function setDisclaimer(data) {
    var el = $("#disclaimer-text");
    if (el && data.meta) {
      el.textContent = data.meta.disclaimer + " · Stand " + data.meta.updated;
    }
  }

  window.CCSOpsPulse = {
    init: function (page) {
      renderNav(page);
      loadData()
        .then(function (data) {
          setDisclaimer(data);
          if (page === "heute") renderHeute(data);
          if (page === "kantinen") renderKantinen(data);
          if (page === "events") renderEvents(data);
          if (page === "nachhaltigkeit") renderNachhaltigkeit(data);
          if (page === "qualitaet") renderQualitaet(data);
        })
        .catch(function (err) {
          console.error(err);
          var main = $(".wrap");
          if (main) {
            main.insertAdjacentHTML("afterbegin",
              '<div class="disclaimer">Daten konnten nicht geladen werden. Bitte Seite neu laden.</div>');
          }
        });
    }
  };
})();
