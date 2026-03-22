// ── Stats strip ───────────────────────────────────────────────────────────────
document.getElementById('stat-median').textContent =
  '£' + STATS.median_price_per_sqm.toLocaleString();
document.getElementById('stat-districts').textContent =
  STATS.num_districts.toLocaleString();
document.getElementById('stat-sales').textContent =
  STATS.total_sales.toLocaleString();
document.getElementById('stat-range').textContent = STATS.date_range;

// ── Tables ────────────────────────────────────────────────────────────────────
function populateTable(id, rows) {
  const tbody = document.querySelector('#' + id + ' tbody');
  rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML =
      `<td><a href="#" class="map-link" data-district="${r.district}">${r.district}</a></td>` +
      `<td>£${r.price_per_sqm.toLocaleString()}</td>`;
    tbody.appendChild(tr);
  });
}
populateTable('tbl-top', STATS.top10);
populateTable('tbl-bottom', STATS.bottom10);

// ── Map initialisation (deferred until GeoJSON is fetched) ───────────────────
async function init() {
  const GEOJSON = await fetch('postcode_districts.geojson').then(r => r.json());

  // ── Colour scale (9-class hybrid, YlOrRd→purple) ─────────────────────────────
  // 7-class quantile breaks for the main distribution (~325 districts each),
  // plus two manual breaks into purple for the high-value tail:
  //   £3,428–£5,000  outer London / expensive commuter belt  (~222 districts)
  //   £5,000–£10,000 inner London / prime regional cities     (~74 districts)
  //   £10,000+       central London only                      (~31 districts)
  const PALETTE = [
    '#ffffb2', '#fed976', '#feb24c', '#fd8d3c',
    '#fc4e2a', '#e31a1c', '#b10026',
    '#7a0177', '#49006a',
  ];

  const allPrices = GEOJSON.features
    .map(f => f.properties.price_per_sqm)
    .filter(v => v != null)
    .sort((a, b) => a - b);

  // 7 quantile lower-bounds, then manual purple thresholds
  const quantileBreaks = [0, 1/7, 2/7, 3/7, 4/7, 5/7, 6/7].map(q => {
    const i = Math.min(Math.floor(q * allPrices.length), allPrices.length - 1);
    return allPrices[i];
  });
  const breaks = [...quantileBreaks, 5000, 10000, allPrices[allPrices.length - 1]];

  function getColour(price) {
    if (price == null) return '#cccccc';
    for (let i = breaks.length - 2; i >= 0; i--) {
      if (price >= breaks[i]) return PALETTE[i];
    }
    return PALETTE[0];
  }

  // ── Price range filter ────────────────────────────────────────────────────────
  let filterLo = 0, filterHi = 100; // percentile indices into allPrices
  let activeBandLo = null, activeBandHi = null; // exact price bounds from legend click

  function priceAtPct(pct) {
    const i = Math.min(Math.floor(pct / 100 * allPrices.length), allPrices.length - 1);
    return allPrices[i];
  }

  function pctForPrice(price) {
    let lo = 0, hi = allPrices.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (allPrices[mid] < price) lo = mid + 1; else hi = mid;
    }
    return Math.round((lo / allPrices.length) * 100);
  }

  function inFilter(price) {
    if (price == null) return false;
    if (activeBandLo !== null)
      return price >= activeBandLo && (activeBandHi === null || price < activeBandHi);
    return price >= priceAtPct(filterLo) && price <= priceAtPct(filterHi);
  }

  // ── Map ───────────────────────────────────────────────────────────────────────
  // Read deep-link param here so we can skip the UK default view when a
  // district is specified — setZoom() fires a zoom animation that completes
  // asynchronously and would override the district fitBounds set in init().
  const initialDistrict = new URLSearchParams(window.location.search).get('postcode');

  const map = L.map('map', {center: [52.5, -1.5], zoom: 6});
  if (!initialDistrict) {
    map.fitBounds([[49.8, -6.5], [55.9, 2.0]]);
    map.setZoom(map.getZoom() + 1);
  }

  const darkMq = window.matchMedia('(prefers-color-scheme: dark)');

  const tileOptions = {
    attribution:
      '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> ' +
      'contributors © <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 19,
  };

  function tileUrl(dark) {
    return `https://{s}.basemaps.cartocdn.com/${dark ? 'dark' : 'light'}_all/{z}/{x}/{y}{r}.png`;
  }

  let tileLayer = L.tileLayer(tileUrl(darkMq.matches), tileOptions).addTo(map);

  darkMq.addEventListener('change', e => {
    tileLayer.remove();
    tileLayer = L.tileLayer(tileUrl(e.matches), tileOptions).addTo(map);
  });

  // Info control (top-right)
  const infoCtrl = L.control({position: 'topright'});
  infoCtrl.onAdd = function () {
    this._div = L.DomUtil.create('div', 'info');
    this._render(null);
    return this._div;
  };
  infoCtrl._render = function (props) {
    if (!props) {
      this._div.innerHTML = '<h4>UK House Prices</h4>Hover over a district';
      return;
    }
    const price = props.price_per_sqm != null
      ? '£' + props.price_per_sqm.toLocaleString() + '/m²'
      : 'No data';
    const sales = props.num_sales != null
      ? props.num_sales.toLocaleString() + ' sales'
      : '';
    this._div.innerHTML =
      `<h4>${props.PostDist}</h4>${price}` +
      (sales ? `<br><span class="muted">Based on ${sales}<span><br><span class="muted">${STATS.date_range}</span>` : '');
  };
  infoCtrl.addTo(map);

  // GeoJSON layer
  let activeLayer = null;

  function districtStyle(feature) {
    const price = feature.properties.price_per_sqm;
    const active = filterLo === 0 && filterHi === 100 || inFilter(price);
    return {
      fillColor: active ? getColour(price) : '#bbbbbb',
      fillOpacity: active ? 0.75 : 0.2,
      color: 'rgba(255,255,255,0.5)',
      weight: 0.5,
    };
  }

  function onMouseover(e) {
    const layer = e.target;
    layer.setStyle({weight: 1.5, color: '#555', fillOpacity: 0.9});
    layer.bringToFront();
    infoCtrl._render(layer.feature.properties);
  }

  function onMouseout(e) {
    if (activeLayer !== e.target) geoLayer.resetStyle(e.target);
    infoCtrl._render(activeLayer ? activeLayer.feature.properties : null);
  }

  function setDistrictParam(code) {
    const p = new URLSearchParams(window.location.search);
    if (code) p.set('postcode', code); else p.delete('postcode');
    const qs = p.toString();
    history.replaceState(null, '', qs ? '?' + qs : window.location.pathname);
  }

  function onClick(e) {
    if (activeLayer) geoLayer.resetStyle(activeLayer);
    activeLayer = e.target;
    activeLayer.setStyle({weight: 2, color: '#333', fillOpacity: 0.9});
    infoCtrl._render(activeLayer.feature.properties);
    setDistrictParam(activeLayer.feature.properties.PostDist);
    L.DomEvent.stopPropagation(e);
  }

  map.on('click', () => {
    if (activeLayer) {geoLayer.resetStyle(activeLayer); activeLayer = null;}
    infoCtrl._render(null);
    setDistrictParam(null);
  });

  const districtLayers = {};

  const geoLayer = L.geoJSON(GEOJSON, {
    renderer: L.canvas(),
    style: districtStyle,
    onEachFeature(feature, layer) {
      districtLayers[feature.properties.PostDist] = layer;
      layer.on({mouseover: onMouseover, mouseout: onMouseout, click: onClick});
    },
  }).addTo(map);

  // ── Deep-link: restore district from ?postcode= on page load ─────────────────
  if (initialDistrict) {
    const layer = districtLayers[initialDistrict];
    if (layer) {
      // Use L.geoJSON(feature).getBounds() rather than layer.getBounds() — with
      // the Canvas renderer, layer._bounds is set lazily on first draw and may
      // be unprojected at this point, causing fitBounds to produce a bad zoom.
      const bounds = L.geoJSON(layer.feature).getBounds();
      map.fitBounds(bounds, {padding: [60, 60], maxZoom: 13});
      activeLayer = layer;
      layer.setStyle({weight: 2, color: '#333', fillOpacity: 0.9});
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
  document.getElementById('price-range-text').innerHTML =
    `Sale prices range from more than £${top2[0].price_per_sqm.toLocaleString()}/m² in ` +
    `${dLink(top2[0].district)} and ${dLink(top2[1].district)}, ` +
    `to under £${bot2[1].price_per_sqm.toLocaleString()}/m² in postcodes like ` +
    `${dLink(bot2[0].district)} and ${dLink(bot2[1].district)}. ` +
    `Zoom to ${cLink('London', 51.51, -0.13, 10)}, ` +
    `${cLink('Birmingham', 52.48, -1.90, 11)}, ` +
    `${cLink('Manchester', 53.48, -2.24, 11)}.`;

  // ── Interesting facts strip ───────────────────────────────────────────────────
  const f = STATS.facts;
  if (f && f.first_non_london) {
    const fnl = f.first_non_london;
    const p1 =
      `Out of ${STATS.num_districts.toLocaleString()} postcode districts analysed, ` +
      `the top ${f.london_streak.toLocaleString()} are all London postcodes and ` +
      `${f.london_in_top_100} of the top 100 are in the capital. ` +
      `The first district with a non-London postcode prefix is ${dLink(fnl.district)} (Richmond), ` +
      `ranked ${fnl.rank}${ordinal(fnl.rank)} at £${fnl.price_per_sqm.toLocaleString()}/m² — ` +
      `technically in Greater London, but the TW area uses a Surrey-style code.`;

    const p2 =
      `Grey districts have no matched sales data. Most of the ${f.no_data_count.toLocaleString()} grey areas ` +
      `are Scottish postcodes — HM Land Registry Price Paid Data covers England and Wales only ` +
      `(with the exception of TD5 and TD9, which appear to have some matched EPC data). ` +
      `Within England and Wales a handful are absent for other reasons: ` +
      `${dLink('TW6')} is Heathrow Airport; ` +
      `${dLink('W1C')} is the heart of Oxford Street, almost entirely commercial; ` +
      `${dLink('PE35')} is the Sandringham Estate; ` +
      `and ${dLink('EC2V')}, ${dLink('EC2N')}, ${dLink('EC2R')}, ${dLink('EC3M')}, ${dLink('EC3V')}, and ${dLink('EC4N')} form the densely ` +
      `financial core of the City of London, where residential properties are rare.`;

    document.getElementById('facts-strip').innerHTML =
      `<p>${p1}</p><p>${p2}</p>`;
  }

  function ordinal(n) {
    const s = ['th', 'st', 'nd', 'rd'];
    const v = n % 100;
    return s[(v - 20) % 10] || s[v] || s[0];
  }

  document.addEventListener('click', function (e) {
    const a = e.target.closest('a.map-link');
    if (!a) return;
    e.preventDefault();
    if (a.dataset.district) {
      const layer = districtLayers[a.dataset.district];
      if (!layer) return;
      setDistrictParam(a.dataset.district);
      map.flyToBounds(layer.getBounds(), {padding: [60, 60], duration: 2.5, maxZoom: 13});
      map.once('moveend', function () {
        if (activeLayer) geoLayer.resetStyle(activeLayer);
        activeLayer = layer;
        layer.setStyle({weight: 2, color: '#333', fillOpacity: 0.9});
        layer.bringToFront();
        infoCtrl._render(layer.feature.properties);
      });
    } else if (a.dataset.lat) {
      map.flyTo(
        [parseFloat(a.dataset.lat), parseFloat(a.dataset.lng)],
        parseInt(a.dataset.zoom),
        {duration: 2.5}
      );
    }
  });

  // Legend (bottom-right)
  const legendCtrl = L.control({position: 'bottomright'});
  legendCtrl.onAdd = function () {
    const div = L.DomUtil.create('div', 'legend');
    L.DomEvent.disableClickPropagation(div);
    L.DomEvent.disableScrollPropagation(div);

    const title = L.DomUtil.create('div', 'legend-title', div);
    title.textContent = '£/m²';

    let activeRow = null;

    PALETTE.forEach((colour, i) => {
      const lo = '£' + Math.round(breaks[i]).toLocaleString();
      const hi = i < PALETTE.length - 1
        ? '–£' + Math.round(breaks[i + 1]).toLocaleString()
        : '+';

      const row = L.DomUtil.create('div', 'legend-row', div);
      row.title = 'Click to filter to this range';
      row.innerHTML =
        `<span class="legend-swatch" style="background:${colour}"></span>` +
        `<span>${lo}${hi}</span>`;

      row.addEventListener('click', () => {
        if (activeRow === row) {
          row.classList.remove('legend-row--active');
          activeRow = null;
          rfLo.value = 0;
          rfHi.value = 100;
          updateFilter(); // clears activeBandLo/Hi and redraws
        } else {
          if (activeRow) activeRow.classList.remove('legend-row--active');
          activeRow = row;
          row.classList.add('legend-row--active');

          // Use exact price bounds for filtering — avoids percentile round-trip error
          activeBandLo = breaks[i];
          activeBandHi = i < PALETTE.length - 1 ? breaks[i + 1] : null;

          // Update slider UI approximately for visual feedback
          const loPct = pctForPrice(breaks[i]);
          const hiPct = i < PALETTE.length - 1 ? pctForPrice(breaks[i + 1]) : 100;
          rfLo.value = loPct;
          rfHi.value = hiPct;
          filterLo = loPct;
          filterHi = hiPct;
          rfFill.style.left = loPct + '%';
          rfFill.style.right = (100 - hiPct) + '%';
          rfLoLabel.textContent = '£' + Math.round(activeBandLo).toLocaleString();
          rfHiLabel.textContent = activeBandHi !== null
            ? '£' + Math.round(activeBandHi).toLocaleString()
            : '£' + Math.round(allPrices[allPrices.length - 1]).toLocaleString();

          const matching = GEOJSON.features.filter(f => {
            const p = f.properties.price_per_sqm;
            return p != null && p >= activeBandLo && (activeBandHi === null || p < activeBandHi);
          });
          if (matching.length > 0) {
            const bounds = L.geoJSON({type: 'FeatureCollection', features: matching}).getBounds();
            map.fitBounds(bounds, {padding: [40, 40], maxZoom: 13});
          }
          geoLayer.setStyle(districtStyle);
        }
      });
    });

    div.insertAdjacentHTML('beforeend', `
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
    </div>`);
    return div;
  };
  legendCtrl.addTo(map);

  // Wire up range filter
  const rfLo = document.getElementById('rf-lo');
  const rfHi = document.getElementById('rf-hi');
  const rfFill = document.getElementById('rf-fill');
  const rfLoLabel = document.getElementById('rf-lo-label');
  const rfHiLabel = document.getElementById('rf-hi-label');

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
    rfFill.style.left = lo + '%';
    rfFill.style.right = (100 - hi) + '%';
    rfLoLabel.textContent = '£' + Math.round(priceAtPct(lo)).toLocaleString();
    rfHiLabel.textContent = '£' + Math.round(priceAtPct(hi)).toLocaleString();
    geoLayer.setStyle(districtStyle);
  }

  rfLo.addEventListener('input', updateFilter);
  rfHi.addEventListener('input', updateFilter);

} // end init()
init();
