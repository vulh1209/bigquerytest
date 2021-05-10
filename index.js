const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery({
  projectId: 'customerlabs-313302',
  keyFilename: 'key.json'
});
const express = require('express')
const app = express()
const port = 3000

async function getevent() {
        const sqlQuery = `SELECT *
            FROM \`customerlabs-313302.customerlabs.events_data\`
            `;

        const options = {
        query: sqlQuery,
        };

        const [rows] = await bigquery.query(options);

        return rows;
    }

app.get('/', (req, res) => {
  res.send('Hello World!')
})

app.get('/event', async (req, res) => {
    const events = await getevent();
    await res.send(events)
})

const server = app.listen(8080, () => {
  const host = server.address().address;
  const port = server.address().port;

  console.log(`Example app listening at http://localhost:${port}`);
});