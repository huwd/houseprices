// shared.js — utilities used by both the postcode district and MSOA pages.
// Loaded as a plain <script> before page-specific scripts; everything here
// is a global available to page.js and msoa.js.

// ── Colour palette (9-class hybrid YlOrRd → purple) ──────────────────────────
// 7-class quantile breaks for the main distribution, plus two manual breaks
// into purple for the high-value tail: £5,000/m² and £10,000/m².
const PALETTE = [
  "#ffffb2",
  "#fed976",
  "#feb24c",
  "#fd8d3c",
  "#fc4e2a",
  "#e31a1c",
  "#b10026",
  "#7a0177",
  "#49006a",
];

/**
 * Build a colour scale from a sorted array of prices.
 *
 * Returns { breaks, getColour } where:
 *   breaks    — 10-element array of price thresholds (7 quantile + 2 fixed + max)
 *   getColour — function(price) → hex string (grey "#cccccc" for null/undefined)
 */
function buildColourScale(sortedPrices) {
  const quantileBreaks = [0, 1 / 7, 2 / 7, 3 / 7, 4 / 7, 5 / 7, 6 / 7].map(
    (q) => {
      const i = Math.min(
        Math.floor(q * sortedPrices.length),
        sortedPrices.length - 1,
      );
      return sortedPrices[i];
    },
  );
  const breaks = [
    ...quantileBreaks,
    5000,
    10000,
    sortedPrices[sortedPrices.length - 1],
  ];

  function getColour(price) {
    if (price == null) return "#cccccc";
    for (let i = breaks.length - 2; i >= 0; i--) {
      if (price >= breaks[i]) return PALETTE[i];
    }
    return PALETTE[0];
  }

  return { breaks, getColour };
}

// ── Ordinal suffix ────────────────────────────────────────────────────────────
function ordinal(n) {
  const s = ["th", "st", "nd", "rd"];
  const v = n % 100;
  return s[(v - 20) % 10] || s[v] || s[0];
}

// ── Ranked table ──────────────────────────────────────────────────────────────
/**
 * Populate a two-column ranked table.
 *
 * @param {string}      id        — id of the <table> element
 * @param {Array}       rows      — array of objects with adj_price_per_sqm and a code field
 * @param {string}      codeKey   — field name for the area code ("district" | "msoa")
 * @param {string}      dataAttr  — data-* attribute name for map links ("data-district" | "data-msoa")
 * @param {string|null} nameKey   — optional field name for human-readable label; falls back to codeKey
 */
function populateTable(id, rows, codeKey, dataAttr, nameKey = null) {
  const tbody = document.querySelector("#" + id + " tbody");
  rows.forEach((r) => {
    const label = (nameKey && r[nameKey]) ? r[nameKey] : r[codeKey];
    const tr = document.createElement("tr");
    tr.innerHTML =
      `<td><a href="#" class="map-link" ${dataAttr}="${r[codeKey]}">${label}</a></td>` +
      `<td>£${r.adj_price_per_sqm.toLocaleString()}</td>`;
    tbody.appendChild(tr);
  });
}

// ── Map tiles (CARTO, dark/light via prefers-color-scheme) ───────────────────
/**
 * Add a CARTO tile layer to the map, swapping dark↔light on system theme change.
 * Returns the MediaQueryList so callers can remove the listener if needed.
 */
function setupMapTiles(map) {
  const tileOptions = {
    attribution:
      '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> ' +
      'contributors © <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: "abcd",
    maxZoom: 19,
  };

  function tileUrl(dark) {
    return `https://{s}.basemaps.cartocdn.com/${dark ? "dark" : "light"}_all/{z}/{x}/{y}{r}.png`;
  }

  const darkMq = window.matchMedia("(prefers-color-scheme: dark)");
  let tileLayer = L.tileLayer(tileUrl(darkMq.matches), tileOptions).addTo(map);

  darkMq.addEventListener("change", (e) => {
    tileLayer.remove();
    tileLayer = L.tileLayer(tileUrl(e.matches), tileOptions).addTo(map);
  });

  return darkMq;
}

// ── Loading overlay ───────────────────────────────────────────────────────────
function dismissLoadingOverlay() {
  const el = document.getElementById("map-loading");
  if (!el) return;
  el.style.opacity = "0";
  el.addEventListener("transitionend", () => el.remove(), { once: true });
}

// ── Point-in-polygon (ray casting) ───────────────────────────────────────────
function pointInRing(lng, lat, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i],
      [xj, yj] = ring[j];
    if (
      yi > lat !== yj > lat &&
      lng < ((xj - xi) * (lat - yi)) / (yj - yi) + xi
    )
      inside = !inside;
  }
  return inside;
}

/**
 * Find the code of the GeoJSON feature containing [lng, lat].
 *
 * @param {number}   lng
 * @param {number}   lat
 * @param {Array}    features   — GeoJSON feature array
 * @param {Function} getCode    — function(feature) → string area code
 * @returns {string|null}
 */
function findFeature(lng, lat, features, getCode) {
  for (const feature of features) {
    const geom = feature.geometry;
    if (!geom) continue;
    let polys = [];
    if (geom.type === "Polygon") polys = [geom.coordinates];
    else if (geom.type === "MultiPolygon") polys = geom.coordinates;
    else if (geom.type === "GeometryCollection")
      geom.geometries.forEach((g) => {
        if (g.type === "Polygon") polys.push(g.coordinates);
        else if (g.type === "MultiPolygon") polys.push(...g.coordinates);
      });
    for (const poly of polys)
      if (pointInRing(lng, lat, poly[0])) return getCode(feature);
  }
  return null;
}
