// Map of one collection and month of the catalogue, on the landing page.
// The page (cdsemeta-index) provides #map, the controls and a JSON list of
// collections and months in <script id="datasets">. Footprints are read from
// the COGP files with @cogp/maplibre: at low zoom only the coarse levels.

import * as maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import maplibreWorkerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url';
import { cogpUrl, registerCogpProtocol } from '@cogp/maplibre';
import './viewer.css';

// MapLibre derives its worker URL from import.meta.url, which breaks once bundled.
maplibregl.setWorkerUrl(maplibreWorkerUrl);
registerCogpProtocol(maplibregl);

const SOURCE = 'footprints';
const LAYER = 'products';
const LAYERS = ['footprints-fill', 'footprints-line'];
// Shown on click. Every mission has these; other columns differ per mission.
const PROPERTIES = { id: 'id', datetime: 'datetime', type: 'product:type', collection: 'collection' };

const $ = (id) => document.getElementById(id);
const { base, datasets } = JSON.parse($('datasets').textContent);
const byId = new Map(datasets.map((d) => [d.id, d]));
const dark = window.matchMedia?.('(prefers-color-scheme: dark)').matches;

const params = new URLSearchParams(location.search);
const initial = byId.get(params.get('c')) ?? byId.get('sentinel-2-l2a') ?? datasets[0];
for (const d of datasets) $('collection').add(new Option(d.id, d.id));
$('collection').value = initial.id;
setMonth(initial, params.get('m'));

const map = new maplibregl.Map({
  container: 'map',
  hash: true,
  style: `https://tiles.openfreemap.org/styles/${dark ? 'dark' : 'positron'}`,
  center: [10, 30],
  zoom: 1.3,
  attributionControl: { compact: true },
});
map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');
map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), 'bottom-left');

function current() {
  return byId.get($('collection').value);
}

/** Pick `wanted` if the collection has it, else its newest month. */
function setMonth(d, wanted) {
  const input = $('month');
  input.min = d.months[0];
  input.max = d.months[d.months.length - 1];
  input.value = d.months.includes(wanted) ? wanted : d.months[d.months.length - 1];
}

function step(n) {
  const d = current();
  const i = d.months.indexOf($('month').value);
  const j = Math.min(d.months.length - 1, Math.max(0, (i < 0 ? d.months.length - 1 : i) + n));
  $('month').value = d.months[j];
  show();
}

function fileUrl() {
  return `${base}/${current().id}.parquet/${$('month').value}.parquet`;
}

function show() {
  const d = current();
  const month = $('month').value;
  const known = d.months.includes(month);
  const i = d.months.indexOf(month);
  $('prev').disabled = i <= 0;
  $('next').disabled = i < 0 || i >= d.months.length - 1;
  $('file').href = known ? fileUrl() : '#';
  $('status').textContent = known ? '' : `No file for ${month}`;
  const q = new URLSearchParams({ c: d.id, m: month });
  try { history.replaceState(null, '', `?${q}${location.hash}`); } catch { /* sandboxed */ }
  if (!map.isStyleLoaded()) return;
  for (const id of LAYERS) if (map.getLayer(id)) map.removeLayer(id);
  if (map.getSource(SOURCE)) map.removeSource(SOURCE);
  if (!known) return;
  map.addSource(SOURCE, {
    type: 'vector',
    tiles: [cogpUrl({ [LAYER]: { url: fileUrl(), properties: PROPERTIES } })],
    minzoom: 0,
    maxzoom: 14,
  });
  const color = dark ? '#7aa2ff' : '#2456d6';
  map.addLayer({
    id: 'footprints-fill', type: 'fill', source: SOURCE, 'source-layer': LAYER,
    // Scenes overlap a lot within a month: keep the fill faint, outlines carry the picture.
    paint: { 'fill-color': color, 'fill-opacity': 0.025 },
  });
  map.addLayer({
    id: 'footprints-line', type: 'line', source: SOURCE, 'source-layer': LAYER,
    paint: {
      'line-color': color,
      'line-opacity': ['interpolate', ['linear'], ['zoom'], 1, 0.25, 6, 0.6],
      'line-width': ['interpolate', ['linear'], ['zoom'], 1, 0.4, 8, 1.2],
    },
  });
}

const escape = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);

map.on('click', 'footprints-fill', (e) => {
  const rows = e.features.slice(0, 8).map((f) => {
    const p = f.properties;
    return `<li><strong>${escape(p.id)}</strong><br>${escape(p.datetime)} · ${escape(p.type)} · ${escape(p.collection)}</li>`;
  });
  const more = e.features.length > 8 ? `<li>… ${e.features.length - 8} more</li>` : '';
  new maplibregl.Popup({ maxWidth: '360px' })
    .setLngLat(e.lngLat)
    .setHTML(`<ul class="products">${rows.join('')}${more}</ul>`)
    .addTo(map);
});
map.on('mouseenter', 'footprints-fill', () => { map.getCanvas().style.cursor = 'pointer'; });
map.on('mouseleave', 'footprints-fill', () => { map.getCanvas().style.cursor = ''; });
map.on('load', show);

$('collection').addEventListener('change', () => { setMonth(current(), $('month').value); show(); });
$('month').addEventListener('change', show);
$('prev').addEventListener('click', () => step(-1));
$('next').addEventListener('click', () => step(1));
