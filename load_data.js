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
    }

    if (!data || !data.features || !Array.isArray(data.features)) {
      console.error('❌ Invalid GeoJSON structure: missing "features" array');
      process.exit(1);
    }

    console.log(`📦 Found ${data.features.length} features`);

    let inserted = 0;
    let skipped = 0;

    for (let i = 0; i < data.features.length; i++) {
      const feature = data.features[i];

      // Guard: must have properties and geometry
      if (!feature || typeof feature !== 'object') {
        console.warn(`⚠️ Skipping feature ${i}: invalid feature object`);
        skipped++;
        continue;
      }

      const props = feature.properties || {};
      const geom = feature.geometry;

      // Guard: must have LineString geometry
      if (!geom || geom.type !== 'LineString') {
        skipped++;
        continue;
      }

      // Guard: coordinates must exist and be array
      if (!geom.coordinates || !Array.isArray(geom.coordinates)) {
        console.warn(`⚠️ Skipping feature ${i}: invalid or missing coordinates`);
        skipped++;
        continue;
      }

      let wkt;
      try {
        // ✅ DEFEND EACH COORDINATE
        const coords = [];
        for (let j = 0; j < geom.coordinates.length; j++) {
          const c = geom.coordinates[j];
          if (!Array.isArray(c) || c.length < 2 || typeof c[0] !== 'number' || typeof c[1] !== 'number') {
            throw new Error(`Invalid coordinate at index ${j}: ${JSON.stringify(c)}`);
          }
          coords.push(`${c[0]} ${c[1]}`);
        }
        wkt = `LINESTRING(${coords.join(', ')})`;
      } catch (coordErr) {
        console.warn(`⚠️ Skipping feature ${i} due to bad coordinates:`, coordErr.message);
        skipped++;
        continue;
      }

      try {
        const query = `
          INSERT INTO parking_rules (
            block_id, street_name, from_address, to_address, side_of_street, direction,
            zone_color, restriction_type, time_range, days_active, meter_rate,
            meter_hours, time_limit, street_cleaning_day, street_cleaning_time,
            effective_dates, geom
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, ST_GeomFromText($17, 4326))
        `;

        // ✅ DEFEND AGAINST UNDEFINED FIELDS
        const values = [
          props.block_id || null,
          props.street_name || null,
          props.from_address || null,
          props.to_address || null,
          props.side_of_street || null,
          props.direction || null,
          props.zone_color || null,
          props.restriction_type || null,
          props.time_range || null,
          props.days_active ? String(props.days_active).split(',').map(s => s.trim()) : [],
          props.meter_rate || null,
          props.meter_hours || null,
          props.time_limit || null,
          props.street_cleaning_day || null,
          props.street_cleaning_time || null,
          props.effective_dates || null,
          wkt
        ];

        await client.query(query, values);
        inserted++;

        // Progress indicator every 500 rows
        if (inserted % 500 === 0) {
          console.log(`↪️  Inserted ${inserted} rules so far...`);
        }

      } catch (rowErr) {
        console.warn(`⚠️ Skipping feature ${i} due to DB error:`, rowErr.message);
        skipped++;
        continue;
      }
    }

    console.log(`✅ INSERTED: ${inserted} parking rules`);
    console.log(`⚠️ SKIPPED: ${skipped} features`);
    await client.end();
    console.log('🔌 Database connection closed');

  } catch (err) {
    console.error('❌ FATAL ERROR:', err.message);
    await client.end().catch(() => {});
    process.exit(1);
  }
}

loadData();