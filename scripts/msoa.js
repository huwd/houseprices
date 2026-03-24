// msoa.js — page-specific script for the MSOA choropleth page.
// Depends on shared.js being loaded first (PALETTE, buildColourScale,
// populateTable, setupMapTiles, dismissLoadingOverlay, findFeature).

// ── Stats strip ───────────────────────────────────────────────────────────────
document.getElementById("stat-median").textContent =
  "£" + STATS.median_price_per_sqm.toLocaleString();
document.getElementById("stat-areas").textContent =
  STATS.num_areas.toLocaleString();
document.getElementById("stat-sales").textContent =
  STATS.total_sales.toLocaleString();
document.getElementById("stat-range").textContent = STATS.date_range;
document.getElementById("stat-cpi-base").textContent = STATS.cpi_base;

// ── Tables ────────────────────────────────────────────────────────────────────
populateTable("tbl-top", STATS.top10, "msoa", "data-msoa", "name");
populateTable("tbl-bottom", STATS.bottom10, "msoa", "data-msoa", "name");

// ── Map initialisation ────────────────────────────────────────────────────────
async function init() {
  const GEOJSON = await fetch("msoa_areas.geojson").then((r) => r.json());

  // Colour scale calibrated on all-area adj_price_per_sqm.
  const allPrices = GEOJSON.features
    .map((f) => f.properties.adj_price_per_sqm)
    .filter((v) => v != null)
    .sort((a, b) => a - b);

  const { breaks, getColour } = buildColourScale(allPrices);

  // Read deep-link param before map init to avoid competing zoom animations.
  const initialMsoa = new URLSearchParams(window.location.search).get("msoa");

  const map = L.map("map", { center: [52.5, -1.5], zoom: 6 });
  if (!initialMsoa) {
    map.fitBounds([
      [49.8, -6.5],
      [55.9, 2.0],
    ]);
    map.setZoom(map.getZoom() + 1);
  }

  setupMapTiles(map);

  // Info control (top-right)
  const infoCtrl = L.control({ position: "topright" });
  infoCtrl.onAdd = function () {
    this._div = L.DomUtil.create("div", "info");
    this._render(null);
    return this._div;
  };
  infoCtrl._render = function (props) {
    if (!props) {
      this._div.innerHTML = "<h4>UK House Prices</h4>Hover over an MSOA";
      return;
    }
    const price = props.adj_price_per_sqm;
    const priceStr =
      price != null ? "£" + price.toLocaleString() + "/m²" : "No data";
    const name = props.MSOA21NM
      ? `<div class="info-area-name">${props.MSOA21NM}</div>`
      : "";
    const sales =
      props.num_sales != null
        ? `<br><span class="muted">Based on ${props.num_sales.toLocaleString()} sales</span>` +
          `<br><span class="muted">${STATS.date_range}</span>`
        : "";
    const rangeNote = `<br><span class="muted">All years · real Jan-2026 £</span>`;
    this._div.innerHTML =
      `<h4>${props.MSOA21CD}</h4>${name}${priceStr}${rangeNote}${sales}`;
  };
  infoCtrl.addTo(map);

  // GeoJSON layer
  let activeLayer = null;

  function msoa_style(feature) {
    const price = feature.properties.adj_price_per_sqm;
    return {
      fillColor: getColour(price),
      fillOpacity: 0.75,
      color: "rgba(255,255,255,0.5)",
      weight: 0.5,
    };
  }

  function onMouseover(e) {
    const layer = e.target;
    layer.setStyle({ weight: 1.5, color: "#555", fillOpacity: 0.9 });
    layer.bringToFront();
    infoCtrl._render(layer.feature.properties);
  }

  function onMouseout(e) {
    if (activeLayer !== e.target) geoLayer.resetStyle(e.target);
    infoCtrl._render(activeLayer ? activeLayer.feature.properties : null);
  }

  function setMsoaParam(code) {
    const p = new URLSearchParams(window.location.search);
    if (code) p.set("msoa", code);
    else p.delete("msoa");
    const qs = p.toString();
    history.replaceState(null, "", qs ? "?" + qs : window.location.pathname);
  }

  function onClick(e) {
    if (activeLayer) geoLayer.resetStyle(activeLayer);
    activeLayer = e.target;
    activeLayer.setStyle({ weight: 2, color: "#333", fillOpacity: 0.9 });
    infoCtrl._render(activeLayer.feature.properties);
    setMsoaParam(activeLayer.feature.properties.MSOA21CD);
    L.DomEvent.stopPropagation(e);
  }

  map.on("click", () => {
    if (activeLayer) {
      geoLayer.resetStyle(activeLayer);
      activeLayer = null;
    }
    infoCtrl._render(null);
    setMsoaParam(null);
  });

  const msoaLayers = {};

  const geoLayer = L.geoJSON(GEOJSON, {
    renderer: L.canvas(),
    style: msoa_style,
    onEachFeature(feature, layer) {
      msoaLayers[feature.properties.MSOA21CD] = layer;
      layer.on({
        mouseover: onMouseover,
        mouseout: onMouseout,
        click: onClick,
      });
    },
  }).addTo(map);

  dismissLoadingOverlay();

  // ── Locate control (top-left, below zoom) ─────────────────────────────────────
  const locateCtrl = L.control({ position: "topleft" });
  locateCtrl.onAdd = function () {
    const div = L.DomUtil.create("div", "leaflet-bar leaflet-control");
    const btn = L.DomUtil.create("a", "locate-btn", div);
    btn.href = "#";
    btn.title = "Find my location";
    btn.setAttribute("role", "button");
    btn.setAttribute("aria-label", "Find my location");
    btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16"
      fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round">
      <circle cx="12" cy="12" r="4"/>
      <line x1="12" y1="2" x2="12" y2="7"/>
      <line x1="12" y1="17" x2="12" y2="22"/>
      <line x1="2" y1="12" x2="7" y2="12"/>
      <line x1="17" y1="12" x2="22" y2="12"/>
    </svg>`;

    L.DomEvent.disableClickPropagation(div);

    L.DomEvent.on(btn, "click", function (e) {
      L.DomEvent.preventDefault(e);
      if (!navigator.geolocation) {
        btn.title = "Geolocation not supported";
        return;
      }
      btn.classList.add("locate-btn--loading");

      function locateError(msg) {
        btn.classList.remove("locate-btn--loading");
        btn.classList.add("locate-btn--error");
        btn.title = msg;
        setTimeout(() => {
          btn.classList.remove("locate-btn--error");
          btn.title = "Find my location";
        }, 3000);
      }

      navigator.geolocation.getCurrentPosition(
        (pos) => {
          btn.classList.remove("locate-btn--loading");
          const { longitude: lng, latitude: lat } = pos.coords;
          const code = findFeature(
            lng,
            lat,
            GEOJSON.features,
            (f) => f.properties.MSOA21CD,
          );
          if (!code) {
            locateError("No MSOA found at your location");
            return;
          }
          const layer = msoaLayers[code];
          if (!layer) {
            locateError("MSOA data unavailable");
            return;
          }
          setMsoaParam(code);
          const bounds = L.geoJSON(layer.feature).getBounds();
          map.flyToBounds(bounds, {
            padding: [60, 60],
            duration: 1.5,
            maxZoom: 13,
          });
          map.once("moveend", function () {
            if (activeLayer) geoLayer.resetStyle(activeLayer);
            activeLayer = layer;
            layer.setStyle({ weight: 2, color: "#333", fillOpacity: 0.9 });
            layer.bringToFront();
            infoCtrl._render(layer.feature.properties);
          });
        },
        (err) => {
          const msg =
            err.code === 1
              ? "Location access denied"
              : err.code === 3
                ? "Location timed out — try again"
                : "Location unavailable";
          locateError(msg);
        },
        { timeout: 30000, maximumAge: 300000 },
      );
    });

    return div;
  };
  locateCtrl.addTo(map);

  // ── Deep-link: restore MSOA from ?msoa= on page load ─────────────────────────
  if (initialMsoa) {
    const layer = msoaLayers[initialMsoa];
    if (layer) {
      const bounds = L.geoJSON(layer.feature).getBounds();
      map.fitBounds(bounds, { padding: [60, 60], maxZoom: 13 });
      activeLayer = layer;
      layer.setStyle({ weight: 2, color: "#333", fillOpacity: 0.9 });
      infoCtrl._render(layer.feature.properties);
    }
  }

  // ── Clickable MSOA links in tables ────────────────────────────────────────────
  document.addEventListener("click", function (e) {
    const a = e.target.closest("a.map-link");
    if (!a || !a.dataset.msoa) return;
    e.preventDefault();
    const layer = msoaLayers[a.dataset.msoa];
    if (!layer) return;
    setMsoaParam(a.dataset.msoa);
    map.flyToBounds(layer.getBounds(), {
      padding: [60, 60],
      duration: 2.5,
      maxZoom: 13,
    });
    map.once("moveend", function () {
      if (activeLayer) geoLayer.resetStyle(activeLayer);
      activeLayer = layer;
      layer.setStyle({ weight: 2, color: "#333", fillOpacity: 0.9 });
      layer.bringToFront();
      infoCtrl._render(layer.feature.properties);
    });
  });

  // ── Price range text ──────────────────────────────────────────────────────────
  const top2 = STATS.top10.slice(0, 2);
  const bot2 = STATS.bottom10.slice(0, 2);
  function msoaLink(entry) {
    const label = entry.name || entry.msoa;
    return `<a href="#" class="map-link" data-msoa="${entry.msoa}">${label}</a>`;
  }
  document.getElementById("price-range-text").innerHTML =
    `Sale prices range from more than £${top2[0].adj_price_per_sqm.toLocaleString()}/m² in ` +
    `${msoaLink(top2[0])} and ${msoaLink(top2[1])}, ` +
    `to under £${bot2[1].adj_price_per_sqm.toLocaleString()}/m² in areas like ` +
    `${msoaLink(bot2[0])} and ${msoaLink(bot2[1])}. ` +
    `All prices are adjusted to ${STATS.cpi_base} pounds using the ONS CPI index.`;

  // ── Legend (bottom-right) ─────────────────────────────────────────────────────
  const legendCtrl = L.control({ position: "bottomright" });
  legendCtrl.onAdd = function () {
    const div = L.DomUtil.create("div", "legend");
    L.DomEvent.disableClickPropagation(div);
    L.DomEvent.disableScrollPropagation(div);

    const title = L.DomUtil.create("div", "legend-title", div);
    title.textContent = "£/m²";

    PALETTE.forEach((colour, i) => {
      const lo = "£" + Math.round(breaks[i]).toLocaleString();
      const hi =
        i < PALETTE.length - 1
          ? "–£" + Math.round(breaks[i + 1]).toLocaleString()
          : "+";

      const row = L.DomUtil.create("div", "legend-row", div);
      row.innerHTML =
        `<span class="legend-swatch" style="background:${colour}"></span>` +
        `<span>${lo}${hi}</span>`;
    });

    return div;
  };
  legendCtrl.addTo(map);
} // end init()
init();
