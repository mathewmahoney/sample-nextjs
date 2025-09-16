const fs = require('fs');
const { Client } = require('pg');

const client = new Client({
  host: 'localhost',
  port: 5432,
  user: 'pguser',
  password: 'pgpass',
  database: 'parkguard'
});

async function loadData() {
  try {
    await client.connect();
    console.log('✅ Connected to PostGIS');

    const rawData = fs.readFileSync('./data/sf_parking.geojson', 'utf8');
    let data;

    try {
      data = JSON.parse(rawData);
    } catch (parseErr) {
      console.error('❌ Failed to parse GeoJSON:', parseErr.message);
      process.exit(1);