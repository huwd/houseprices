// ── Feature flags ─────────────────────────────────────────────────────────────
const FEATURES = {
  gpsLocate: true,
};

// ── Stats strip ───────────────────────────────────────────────────────────────
document.getElementById("stat-median").textContent =
  "£" + STATS.median_price_per_sqm.toLocaleString();
document.getElementById("stat-districts").textContent =
  STATS.num_districts.toLocaleString();
document.getElementById("stat-sales").textContent =
  STATS.total_sales.toLocaleString();
document.getElementById("stat-range").textContent = STATS.date_range;
document.getElementById("stat-cpi-base").textContent = STATS.cpi_base;

// ── Tables ────────────────────────────────────────────────────────────────────
function populateTable(id, rows) {
  const tbody = document.querySelector("#" + id + " tbody");
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    tr.innerHTML =
      `<td><a href="#" class="map-link" data-district="${r.district}">${r.district}</a></td>` +
      `<td>£${r.price_per_sqm.toLocaleString()}</td>`;
    tbody.appendChild(tr);
  });
}
populateTable("tbl-top", STATS.top10);
populateTable("tbl-bottom", STATS.bottom10);

// ── Map initialisation (deferred until GeoJSON is fetched) ───────────────────
async function init() {
  const [GEOJSON, YEARLY] = await Promise.all([
    fetch("postcode_districts.geojson").then((r) => r.json()),
    fetch("yearly_totals.json")
      .then((r) => r.json())
      .catch(() => null),
  ]);

  // ── Colour scale (9-class hybrid, YlOrRd→purple) ─────────────────────────────
  // 7-class quantile breaks for the main distribution (~325 districts each),
  // plus two manual breaks into purple for the high-value tail:
  //   £3,428–£5,000  outer London / expensive commuter belt  (~222 districts)
  //   £5,000–£10,000 inner London / prime regional cities     (~74 districts)
  //   £10,000+       central London only                      (~31 districts)
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

  const allPrices = GEOJSON.features
    .map((f) => f.properties.adj_price_per_sqm)
    .filter((v) => v != null)
    .sort((a, b) => a - b);

  // 7 quantile lower-bounds, then manual purple thresholds
  const quantileBreaks = [0, 1 / 7, 2 / 7, 3 / 7, 4 / 7, 5 / 7, 6 / 7].map(
    (q) => {
      const i = Math.min(
        Math.floor(q * allPrices.length),
        allPrices.length - 1,
      );
      return allPrices[i];
    },
  );
  const breaks = [
    ...quantileBreaks,
    5000,
    10000,
    allPrices[allPrices.length - 1],
  ];

  function getColour(price) {
    if (price == null) return "#cccccc";
    for (let i = breaks.length - 2; i >= 0; i--) {
      if (price >= breaks[i]) return PALETTE[i];
    }
    return PALETTE[0];
  }

  // ── Year range filter ─────────────────────────────────────────────────────────
  const yrMaxYear = YEARLY ? Math.max(...Object.values(YEARLY.districts).flatMap((d) => Object.keys(d).map(Number))) : new Date().getFullYear();
  let yearStart = YEARLY ? YEARLY.min_year : null;
  let yearEnd = YEARLY ? yrMaxYear : null;

  function isYearFiltered() {
    return (
      YEARLY !== null &&
      yearStart !== null &&
      !(yearStart === YEARLY.min_year && yearEnd === yrMaxYear)
    );
  }

  function computeYearlyPrice(district) {
    if (!YEARLY || !isYearFiltered()) return null;
    const data = YEARLY.districts[district];
    if (!data) return null;
    let totalW = 0,
      totalFA = 0;
    for (let yr = yearStart; yr <= yearEnd; yr++) {
      const d = data[yr];
      if (d) {
        totalW += d.p * d.fa;
        totalFA += d.fa;
      }
    }
    return totalFA > 0 ? Math.round(totalW / totalFA) : null;
  }

  // ── Price range filter ────────────────────────────────────────────────────────
  let filterLo = 0,
    filterHi = 100; // percentile indices into allPrices
  let activeBandLo = null,
    activeBandHi = null; // exact price bounds from legend click

  function priceAtPct(pct) {
    const i = Math.min(
      Math.floor((pct / 100) * allPrices.length),
      allPrices.length - 1,
    );
    return allPrices[i];
  }

  function pctForPrice(price) {
    let lo = 0,
      hi = allPrices.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (allPrices[mid] < price) lo = mid + 1;
      else hi = mid;
    }
    return Math.round((lo / allPrices.length) * 100);
  }

  function inFilter(price) {
    if (price == null) return false;
    if (activeBandLo !== null)
      return (
        price >= activeBandLo && (activeBandHi === null || price < activeBandHi)
      );
    return price >= priceAtPct(filterLo) && price <= priceAtPct(filterHi);
  }

  // ── Map ───────────────────────────────────────────────────────────────────────
  // Read deep-link param here so we can skip the UK default view when a
  // district is specified — setZoom() fires a zoom animation that completes
  // asynchronously and would override the district fitBounds set in init().
  const initialDistrict = new URLSearchParams(window.location.search).get(
    "postcode",
  );

  const map = L.map("map", { center: [52.5, -1.5], zoom: 6 });
  if (!initialDistrict) {
    map.fitBounds([
      [49.8, -6.5],
      [55.9, 2.0],
    ]);
    map.setZoom(map.getZoom() + 1);
  }

  const darkMq = window.matchMedia("(prefers-color-scheme: dark)");

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

  let tileLayer = L.tileLayer(tileUrl(darkMq.matches), tileOptions).addTo(map);

  darkMq.addEventListener("change", (e) => {
    tileLayer.remove();
    tileLayer = L.tileLayer(tileUrl(e.matches), tileOptions).addTo(map);
  });

  // Info control (top-right)
  const infoCtrl = L.control({ position: "topright" });
  infoCtrl.onAdd = function () {
    this._div = L.DomUtil.create("div", "info");
    this._render(null);
    return this._div;
  };
  infoCtrl._render = function (props) {
    if (!props) {
      this._div.innerHTML = "<h4>UK House Prices</h4>Hover over a district";
      return;
    }
    const yearlyPrice = computeYearlyPrice(props.PostDist);
    const displayPrice = yearlyPrice !== null ? yearlyPrice : props.adj_price_per_sqm;
    const priceStr =
      displayPrice != null
        ? "£" + displayPrice.toLocaleString() + "/m²"
        : "No data";
    const rangeNote =
      yearlyPrice !== null
        ? `<br><span class="muted">${yearStart}–${yearEnd} avg</span>`
        : `<br><span class="muted">All years (real Jan-2026 £)</span>`;
    const sales =
      !yearlyPrice && props.num_sales != null
        ? props.num_sales.toLocaleString() + " sales"
        : "";
    this._div.innerHTML =
      `<h4>${props.PostDist}</h4>${priceStr}${rangeNote}` +
      (sales
        ? `<br><span class="muted">Based on ${sales}</span><br><span class="muted">${STATS.date_range}</span>`
        : "");
  };
  infoCtrl.addTo(map);

  // GeoJSON layer
  let activeLayer = null;

  function districtStyle(feature) {
    const district = feature.properties.PostDist;
    const price = isYearFiltered()
      ? computeYearlyPrice(district)
      : feature.properties.adj_price_per_sqm;
    const active = (filterLo === 0 && filterHi === 100) || inFilter(price);
    return {
      fillColor: active ? getColour(price) : "#bbbbbb",
      fillOpacity: active ? 0.75 : 0.2,
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

  function setDistrictParam(code) {
    const p = new URLSearchParams(window.location.search);
    if (code) p.set("postcode", code);
    else p.delete("postcode");
    const qs = p.toString();
    history.replaceState(null, "", qs ? "?" + qs : window.location.pathname);
  }

  function onClick(e) {
    if (activeLayer) geoLayer.resetStyle(activeLayer);
    activeLayer = e.target;
    activeLayer.setStyle({ weight: 2, color: "#333", fillOpacity: 0.9 });
    infoCtrl._render(activeLayer.feature.properties);
    setDistrictParam(activeLayer.feature.properties.PostDist);
    L.DomEvent.stopPropagation(e);
  }

  map.on("click", () => {
    if (activeLayer) {
      geoLayer.resetStyle(activeLayer);
      activeLayer = null;
    }
    infoCtrl._render(null);
    setDistrictParam(null);
  });

  const districtLayers = {};

  const geoLayer = L.geoJSON(GEOJSON, {
    renderer: L.canvas(),
    style: districtStyle,
    onEachFeature(feature, layer) {
      districtLayers[feature.properties.PostDist] = layer;
      layer.on({
        mouseover: onMouseover,
        mouseout: onMouseout,
        click: onClick,
      });
    },
  }).addTo(map);

  // ── Dismiss loading overlay ───────────────────────────────────────────────────
  const mapLoading = document.getElementById("map-loading");
  if (mapLoading) {
    mapLoading.style.opacity = "0";
    mapLoading.addEventListener("transitionend", () => mapLoading.remove(), {
      once: true,
    });
  }

  // ── Point-in-polygon (ray casting) for locate ────────────────────────────────
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

  function findDistrict(lng, lat) {
    for (const feature of GEOJSON.features) {
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
        if (pointInRing(lng, lat, poly[0])) return feature.properties.PostDist;
    }
    return null;
  }

  // ── Locate control (top-left, below zoom) ─────────────────────────────────────
  // Disabled: see issue #93
  if (FEATURES.gpsLocate) {
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
            const code = findDistrict(lng, lat);
            if (!code) {
              locateError("No district found at your location");
              return;
            }
            const layer = districtLayers[code];
            if (!layer) {
              locateError("District data unavailable");
              return;
            }
            setDistrictParam(code);
            // Use L.geoJSON().getBounds() — Canvas renderer sets layer._bounds lazily
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
  } // end FEATURES.gpsLocate

  // ── Search control (top-left) ─────────────────────────────────────────────────
  const searchCtrl = L.control({ position: "topleft" });
  searchCtrl.onAdd = function () {
    const div = L.DomUtil.create("div", "search-control");
    L.DomEvent.disableClickPropagation(div);
    L.DomEvent.disableScrollPropagation(div);

    const input = L.DomUtil.create("input", "", div);
    input.type = "text";
    input.placeholder = "Search postcode…";
    input.setAttribute("aria-label", "Search postcode district");

    const suggestions = L.DomUtil.create("div", "search-suggestions", div);
    suggestions.style.display = "none";

    let activeIdx = -1;

    function navigateTo(code) {
      const layer = districtLayers[code];
      if (!layer) return;
      input.value = "";
      suggestions.style.display = "none";
      activeIdx = -1;
      setDistrictParam(code);
      map.flyToBounds(layer.getBounds(), {
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
    }

    function updateSuggestions() {
      const q = input.value.trim().toUpperCase();
      suggestions.innerHTML = "";
      activeIdx = -1;
      if (!q) {
        suggestions.style.display = "none";
        return;
      }

      const matches = Object.keys(districtLayers)
        .filter((k) => k.startsWith(q))
        .sort()
        .slice(0, 7);

      if (!matches.length) {
        suggestions.style.display = "none";
        return;
      }

      matches.forEach((code) => {
        const item = L.DomUtil.create("div", "search-suggestion", suggestions);
        item.textContent = code;
        item.addEventListener("mousedown", (e) => {
          e.preventDefault(); // prevent input blur before click fires
          navigateTo(code);
        });
      });
      suggestions.style.display = "block";
    }

    input.addEventListener("input", updateSuggestions);

    input.addEventListener("keydown", function (e) {
      const items = suggestions.querySelectorAll(".search-suggestion");
      if (!items.length) return;

      if (e.key === "ArrowDown") {
        e.preventDefault();
        activeIdx = Math.min(activeIdx + 1, items.length - 1);
        items.forEach((el, i) =>
          el.classList.toggle("search-suggestion--active", i === activeIdx),
        );
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        activeIdx = Math.max(activeIdx - 1, 0);
        items.forEach((el, i) =>
          el.classList.toggle("search-suggestion--active", i === activeIdx),
        );
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (activeIdx >= 0) {
          navigateTo(items[activeIdx].textContent);
        } else if (items.length === 1) {
          navigateTo(items[0].textContent);
        } else {
          const exact = input.value.trim().toUpperCase();
          if (districtLayers[exact]) navigateTo(exact);
        }
      } else if (e.key === "Escape") {
        input.value = "";
        suggestions.style.display = "none";
        activeIdx = -1;
      }
    });

    input.addEventListener("blur", () => {
      // Small delay so mousedown on a suggestion fires before blur hides it
      setTimeout(() => {
        suggestions.style.display = "none";
        activeIdx = -1;
      }, 150);
    });

    return div;
  };
  searchCtrl.addTo(map);

  // ── Deep-link: restore district from ?postcode= on page load ─────────────────
  if (initialDistrict) {
    const layer = districtLayers[initialDistrict];
    if (layer) {
      // Use L.geoJSON(feature).getBounds() rather than layer.getBounds() — with
      // the Canvas renderer, layer._bounds is set lazily on first draw and may
      // be unprojected at this point, causing fitBounds to produce a bad zoom.
      const bounds = L.geoJSON(layer.feature).getBounds();
      map.fitBounds(bounds, { padding: [60, 60], maxZoom: 13 });
      activeLayer = layer;
      layer.setStyle({ weight: 2, color: "#333", fillOpacity: 0.9 });
      infoCtrl._render(layer.feature.properties);
    }
  }

  // ── Clickable district / city links ───────────────────────────────────────────
  function dLink(code) {
    return `<a href="#" class="map-link" data-district="${code}">${code}</a>`;
  }

  function cLink(name, lat, lng, zoom) {
    return `<a href="#" class="map-link" data-lat="${lat}" data-lng="${lng}" data-zoom="${zoom}">${name}</a>`;
  }

  const top2 = STATS.top10.slice(0, 2);
  const bot2 = STATS.bottom10.slice(0, 2);
  document.getElementById("price-range-text").innerHTML =
    `Sale prices range from more than £${top2[0].price_per_sqm.toLocaleString()}/m² in ` +
    `${dLink(top2[0].district)} and ${dLink(top2[1].district)}, ` +
    `to under £${bot2[1].price_per_sqm.toLocaleString()}/m² in postcodes like ` +
    `${dLink(bot2[0].district)} and ${dLink(bot2[1].district)}. ` +
    `Zoom to ${cLink("London", 51.51, -0.13, 10)}, ` +
    `${cLink("Birmingham", 52.48, -1.9, 11)}, ` +
    `${cLink("Manchester", 53.48, -2.24, 11)}. ` +
    `All historic prices have been adjusted for inflation to ${STATS.cpi_base} pounds using the ONS CPI index.`;

  // ── Interesting facts strip ───────────────────────────────────────────────────
  const f = STATS.facts;
  if (f && f.first_non_london) {
    const fnl = f.first_non_london;
    const nonLon = f.non_london_top_100 || [];
    const nonLonFmt = nonLon.map(
      (d) => `${dLink(d.district)} (${d.rank}${ordinal(d.rank)})`,
    );
    const nonLonStr =
      nonLon.length === 0
        ? ""
        : nonLon.length === 1
          ? ` — the only exception is ${nonLonFmt[0]}`
          : ` — the only exceptions are ${nonLonFmt.slice(0, -1).join(", ")} and ${nonLonFmt[nonLonFmt.length - 1]}`;
    const lonTop100Str =
      f.london_in_top_100 === 100
        ? `all of the top 100 are within Greater London`
        : `Greater London accounts for ${f.london_in_top_100} of the top 100${nonLonStr}`;
    const p1 =
      `Out of ${STATS.num_districts.toLocaleString()} postcode districts analysed, ` +
      `the top ${f.london_streak.toLocaleString()} are all London postcodes and ${lonTop100Str}. ` +
      `The first district outside Greater London is ${dLink(fnl.district)} (Cambridge), ` +
      `ranked ${fnl.rank}${ordinal(fnl.rank)} at £${fnl.price_per_sqm.toLocaleString()}/m².`;

    const p2 =
      `Grey districts have no matched sales data. Most of the ${f.no_data_count.toLocaleString()} grey areas ` +
      `are Scottish postcodes — HM Land Registry Price Paid Data covers England and Wales only ` +
      `(with the exception of ${dLink("TD5")} and ${dLink("TD9")}, which appear to have some matched EPC data).`;

    const p3 =
      `Within England and Wales a handful are absent for other reasons: ` +
      `${dLink("TW6")} is Heathrow Airport; ` +
      `${dLink("W1C")} is the heart of Oxford Street, almost entirely commercial; ` +
      `${dLink("PE35")} is the Sandringham Estate; ` +
      `${dLink("TR23")} is Bryher in the Isles of Scilly, with fewer than ten recorded transactions; ` +
      `and ${dLink("EC2V")}, ${dLink("EC2N")}, ${dLink("EC2R")}, ${dLink("EC3M")}, ${dLink("EC3V")}, and ${dLink("EC4N")} form the densely ` +
      `financial core of the City of London, where residential properties are also rare.`;

    const missing = STATS.missing_geometry || [];
    const p4 =
      missing.length === 0
        ? ""
        : missing.length === 1
          ? `${dLink(missing[0].district)} has ${missing[0].num_sales.toLocaleString()} matched sales ` +
            `(inflation-adjusted £${missing[0].adj_price_per_sqm.toLocaleString()}/m²) but does not appear on the map. ` +
            `The postcode district was created after our boundary source was last updated in 2012 and no polygon exists for it yet. ` +
            `I've tried, but I cannot find a legal way to add the boundary from publicly available data without violating Royal Mail's licensing terms. ` +
            `If you also find that frustrating, <a href="https://takes.jamesomalley.co.uk/p/heres-the-plan-to-actually-liberate?ref=houseprices.huwdiprose.co.uk">Free the PAF</a>!`
          : `${missing.length} districts have matched sales data but no boundary geometry and do not appear on the map: ` +
            missing
              .map(
                (d) =>
                  `${dLink(d.district)} (${d.num_sales.toLocaleString()} sales)`,
              )
              .join(", ") +
            ".";

    document.getElementById("facts-strip").innerHTML =
      `<p>${p1}</p><p>${p2}</p><p>${p3}</p>` + (p4 ? `<p>${p4}</p>` : "");
  }

  function ordinal(n) {
    const s = ["th", "st", "nd", "rd"];
    const v = n % 100;
    return s[(v - 20) % 10] || s[v] || s[0];
  }

  document.addEventListener("click", function (e) {
    const a = e.target.closest("a.map-link");
    if (!a) return;
    e.preventDefault();
    if (a.dataset.district) {
      const layer = districtLayers[a.dataset.district];
      if (!layer) return;
      setDistrictParam(a.dataset.district);
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
    } else if (a.dataset.lat) {
      map.flyTo(
        [parseFloat(a.dataset.lat), parseFloat(a.dataset.lng)],
        parseInt(a.dataset.zoom),
        { duration: 2.5 },
      );
    }
  });

  // Legend (bottom-right)
  const legendCtrl = L.control({ position: "bottomright" });
  legendCtrl.onAdd = function () {
    const div = L.DomUtil.create("div", "legend");
    L.DomEvent.disableClickPropagation(div);
    L.DomEvent.disableScrollPropagation(div);

    const title = L.DomUtil.create("div", "legend-title", div);
    title.textContent = "£/m²";

    let activeRow = null;

    PALETTE.forEach((colour, i) => {
      const lo = "£" + Math.round(breaks[i]).toLocaleString();
      const hi =
        i < PALETTE.length - 1
          ? "–£" + Math.round(breaks[i + 1]).toLocaleString()
          : "+";

      const row = L.DomUtil.create("div", "legend-row", div);
      row.title = "Click to filter to this range";
      row.innerHTML =
        `<span class="legend-swatch" style="background:${colour}"></span>` +
        `<span>${lo}${hi}</span>`;

      row.addEventListener("click", () => {
        if (activeRow === row) {
          row.classList.remove("legend-row--active");
          activeRow = null;
          rfLo.value = 0;
          rfHi.value = 100;
          updateFilter(); // clears activeBandLo/Hi and redraws
        } else {
          if (activeRow) activeRow.classList.remove("legend-row--active");
          activeRow = row;
          row.classList.add("legend-row--active");

          // Use exact price bounds for filtering — avoids percentile round-trip error
          activeBandLo = breaks[i];
          activeBandHi = i < PALETTE.length - 1 ? breaks[i + 1] : null;

          // Update slider UI approximately for visual feedback
          const loPct = pctForPrice(breaks[i]);
          const hiPct =
            i < PALETTE.length - 1 ? pctForPrice(breaks[i + 1]) : 100;
          rfLo.value = loPct;
          rfHi.value = hiPct;
          filterLo = loPct;
          filterHi = hiPct;
          rfFill.style.left = loPct + "%";
          rfFill.style.right = 100 - hiPct + "%";
          rfLoLabel.textContent =
            "£" + Math.round(activeBandLo).toLocaleString();
          rfHiLabel.textContent =
            activeBandHi !== null
              ? "£" + Math.round(activeBandHi).toLocaleString()
              : "£" +
                Math.round(allPrices[allPrices.length - 1]).toLocaleString();

          const matching = GEOJSON.features.filter((f) => {
            const p = f.properties.adj_price_per_sqm;
            return (
              p != null &&
              p >= activeBandLo &&
              (activeBandHi === null || p < activeBandHi)
            );
          });
          if (matching.length > 0) {
            const bounds = L.geoJSON({
              type: "FeatureCollection",
              features: matching,
            }).getBounds();
            map.fitBounds(bounds, { padding: [40, 40], maxZoom: 13 });
          }
          geoLayer.setStyle(districtStyle);
        }
      });
    });

    div.insertAdjacentHTML(
      "beforeend",
      `
    <div class="range-filter">
      <div class="range-wrap">
        <div class="range-track"><div class="range-fill" id="rf-fill"></div></div>
        <input type="range" id="rf-lo" min="0" max="100" value="0" step="1">
        <input type="range" id="rf-hi" min="0" max="100" value="100" step="1">
      </div>
      <div class="range-labels">
        <span id="rf-lo-label">£${Math.round(allPrices[0]).toLocaleString()}</span>
        <span id="rf-hi-label">£${Math.round(allPrices[allPrices.length - 1]).toLocaleString()}</span>
      </div>
    </div>`,
    );
    return div;
  };
  legendCtrl.addTo(map);

  // Wire up range filter
  const rfLo = document.getElementById("rf-lo");
  const rfHi = document.getElementById("rf-hi");
  const rfFill = document.getElementById("rf-fill");
  const rfLoLabel = document.getElementById("rf-lo-label");
  const rfHiLabel = document.getElementById("rf-hi-label");

  function updateFilter() {
    activeBandLo = null;
    activeBandHi = null;
    let lo = parseInt(rfLo.value);
    let hi = parseInt(rfHi.value);
    if (lo > hi) {
      if (document.activeElement === rfLo) rfLo.value = lo = hi;
      else rfHi.value = hi = lo;
    }
    filterLo = lo;
    filterHi = hi;
    rfFill.style.left = lo + "%";
    rfFill.style.right = 100 - hi + "%";
    rfLoLabel.textContent = "£" + Math.round(priceAtPct(lo)).toLocaleString();
    rfHiLabel.textContent = "£" + Math.round(priceAtPct(hi)).toLocaleString();
    geoLayer.setStyle(districtStyle);
  }

  rfLo.addEventListener("input", updateFilter);
  rfHi.addEventListener("input", updateFilter);

  // ── Year range slider ─────────────────────────────────────────────────────────
  if (YEARLY && YEARLY.min_year) {
    const yrFilter = document.getElementById("year-filter");
    const yrStartEl = document.getElementById("yr-start");
    const yrEndEl = document.getElementById("yr-end");
    const yrStartVal = document.getElementById("yr-start-val");
    const yrEndVal = document.getElementById("yr-end-val");
    const yrReset = document.getElementById("yr-reset");

    yrStartEl.min = YEARLY.min_year;
    yrStartEl.max = yrMaxYear;
    yrStartEl.value = YEARLY.min_year;
    yrEndEl.min = YEARLY.min_year;
    yrEndEl.max = yrMaxYear;
    yrEndEl.value = yrMaxYear;

    function updateYearRange() {
      let s = parseInt(yrStartEl.value);
      let e = parseInt(yrEndEl.value);
      if (s > e) {
        if (document.activeElement === yrStartEl) yrStartEl.value = s = e;
        else yrEndEl.value = e = s;
      }
      yearStart = s;
      yearEnd = e;
      yrStartVal.textContent = s;
      yrEndVal.textContent = e;
      const allTime = s === YEARLY.min_year && e === yrMaxYear;
      yrReset.style.display = allTime ? "none" : "inline";
      geoLayer.setStyle(districtStyle);
      // refresh info panel if a district is selected
      if (activeLayer) infoCtrl._render(activeLayer.feature.properties);
    }

    yrStartEl.addEventListener("input", updateYearRange);
    yrEndEl.addEventListener("input", updateYearRange);
    yrReset.addEventListener("click", () => {
      yrStartEl.value = YEARLY.min_year;
      yrEndEl.value = yrMaxYear;
      updateYearRange();
    });

    yrFilter.style.display = "flex";
  }
} // end init()
init();
