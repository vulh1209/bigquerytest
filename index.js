const { BigQuery } = require("@google-cloud/bigquery");
const express = require("express");
const schedule = require("node-schedule");
const cors = require("cors");
const path = require("path");
jsonfile = path.resolve("./json/test.json");
RawLeadFile = path.resolve("./json/Data.json");
const fs = require("fs");
const puppeteer = require("puppeteer");
const querystring = require("querystring");

const bigquery = new BigQuery({
  projectId: "customerlabs-313302",
  keyFilename: "key.json",
});

const app = express();
const port = 8080;
app.use(cors());
const datasetId = "report";
const tableId = "leads";

const job = schedule.scheduleJob("* /1 * * * *", function () {
  console.log("test schedule");
});

const groupBy = (array, key) => {
  return array.reduce(function (rv, x) {
    (rv[x[key]] = rv[x[key]] || []).push(x);
    return rv;
  }, {});
};

const getevent = async () => {
  const sqlQuery = `SELECT * FROM \`customerlabs-313302.customerlabs.events_data\``;

  const options = {
    query: sqlQuery,
  };

  const [rows] = await bigquery.query(options);

  return rows;
};

const getrawlead = async () => {
  const sqlQuery = `SELECT * FROM \`customerlabs-313302.customerlabs.users_data\` ORDER BY inserted_at DESC`;

  const options = {
    query: sqlQuery,
  };

  const [rows] = await bigquery.query(options);

  return rows;
};

const clearData = async (ds, dt) => {
  const sqlQuery = `CREATE OR REPLACE TABLE \`customerlabs-313302.${ds}.${dt}\`  AS SELECT * FROM \`customerlabs-313302.${ds}.${dt}\` LIMIT 0;`;

  const options = {
    query: sqlQuery,
  };

  const [rows] = await bigquery.query(options);

  return rows;
};

const inserttest = async () => {
  const sqlQuery = `INSERT INTO \`test_dataset.test_table\` VALUES(‘SG Note 10’)`;

  const options = {
    query: sqlQuery,
  };

  const [rows] = await bigquery.query(options);

  return rows;
};

app.get("/", (req, res) => {
  let p1 = req.query.p1;
  let p2 = req.query.p2;
  res.send(`start ${p1} - ${p2}`);
});

app.get("/submitform/", async (req, res) => {
  const rawUrl = req.params.query;

  const name = req.query.name;
  const phone = req.query.phone;
  const email = req.query.email;
  const note = req.query.note;


  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();
  page.setViewport({ width: 1280, height: 720 });
  await page.goto("http://localhost:3000/", { waitUntil: "networkidle2" });

  await page.type("#Name", name);
  await page.type("#Phone", phone);
  await page.type("#Email", email);
  await page.type("#Note", note);
  await page.click("#submit");
  await page.waitForTimeout(5000);
  await browser.close();
  res.send({name:name,phone:phone,email:email,note:note});
  // res.download(pic)
});

app.get("/test", async (req, res) => {
  const jsondata = [
    {
      user_id: "cl3855n4t16llg4b0fb092-3ce7-4c1f-b662-246ecd3b071a",
      email: "khanh.td@ttgvn.com",
    },
    {
      user_id: "cl3855n4t16llgce766df6-b47a-4353-86c1-76064b201f53",
      email: "tranthachthao1995@gmail.com",
    },
  ];
  try {
    const ndJson = jsondata.map(JSON.stringify).join("\n");
    await fs.writeFileSync("./json/data.json", ndJson);
    res.send("test worked!");
  } catch (err) {
    res.send(err);
  }
});

app.get("/insert", async (req, res) => {
  const result = await inserttest();
  res.send(result);
});

app.get("/create/:dsname", async (req, res) => {
  // Specify the geographic location where the dataset should reside
  const dsname = req.params.dsname;
  const options = {
    location: "asia-southeast1",
  };

  // Create a new dataset
  const [dataset] = await bigquery.createDataset(dsname, options);
  console.log(`Dataset ${dataset.id} created.`);
  res.send(`Dataset ${dataset.id} created.`);
});

app.get("/create/:dsname/:dtname", async (req, res) => {
  // Specify the geographic location where the dataset should reside
  const dsname = req.params.dsname;
  const dtname = req.params.dtname;
  const options = {
    location: "asia-southeast1",
  };

  // Create a new dataset
  const [table] = await bigquery.dataset(dsname).createTable(dtname, options);

  console.log(`Table ${table.id} created.`);
  res.send(`Table ${table.id} created.`);
});

app.get("/import/:dsname/:dtname", async (req, res) => {
  // Specify the geographic location where the dataset should reside
  const dsname = req.params.dsname;
  const dtname = req.params.dtname;

  await clearData(dsname, dtname);

  const options = {
    sourceFormat: "NEWLINE_DELIMITED_JSON",
    schema: {
      fields: [
        { name: "userid", type: "STRING" },
        { name: "email", type: "STRING" },
      ],
    },
    location: "asia-southeast1",
  };

  // const jsondata = [
  //   {
  //     user_id: "cl3855n4t16llg4b0fb092-3ce7-4c1f-b662-246ecd3b071a",
  //     email: "khanh.td@ttgvn.com"
  //   },
  //   {
  //     user_id: "cl3855n4t16llgce766df6-b47a-4353-86c1-76064b201f53",
  //     email: "tranthachthao1995@gmail.com"
  //   },
  // ];

  // const jsonfile = "./json/test.csv";

  // await bigquery
  //   .createDataset('test_dataset', options)

  // await bigquery
  //   .dataset('test_dataset')
  //   .createTable('test_table', options)
  try {
    const [job] = await bigquery
      .dataset(dsname)
      .table(dtname)
      .load(jsonfile, options);
    console.log(`Job ${job.id} completed.`);

    // Check the job's status for errors
    const errors = job.status.errors;
    if (errors && errors.length > 0) {
      throw errors;
    } else {
      res.send(job);
    }
  } catch (err) {
    res.send(err);
  }
});

app.get("/all", async (req, res) => {
  const events = await getevent();
  const rawleads = await getrawlead();
  res.send({ rawleads: rawleads, events: events });
});

app.get("/score", async (req, res) => {
  const events = await getevent();
  const rawleads = await getrawlead();
  // const gb_rawleads = groupBy(rawleads, "user_id"); // groupby raw lead

  const result = rawleads.map((lead) => {
    return {
      user_id: lead.user_id,
      fullname: lead.traits.filter((obj) => {
        return obj.key === "fullname";
      })[0]
        ? lead.traits.filter((obj) => {
            return obj.key === "fullname";
          })[0].value
        : "",
      email: lead.external_ids.filter((obj) => {
        return obj.key === "identify_by_email";
      })[0]
        ? lead.external_ids.filter((obj) => {
            return obj.key === "identify_by_email";
          })[0].value
        : "",
      phone: lead.external_ids.filter((obj) => {
        return obj.key === "identify_by_phone";
      })[0]
        ? lead.external_ids.filter((obj) => {
            return obj.key === "identify_by_phone";
          })[0].value
        : "",
      fb_id: lead.external_ids.filter((obj) => {
        return obj.key === "_fbp";
      })[0]
        ? lead.external_ids.filter((obj) => {
            return obj.key === "_fbp";
          })[0].value
        : "",
      google_analytic_id: lead.external_ids.filter((obj) => {
        return obj.key === "client_id";
      })[0]
        ? lead.external_ids.filter((obj) => {
            return obj.key === "client_id";
          })[0].value
        : "",
      score_hcm: lead.additional_info.filter((obj) => {
        return obj.key === "city" && obj.value === "Ciudad Ho Chi Minh";
      }).length,
      score_hn: lead.additional_info.filter((obj) => {
        return obj.key === "city" && obj.value === "Hanoi";
      }).length,
      score_pageview: events.filter((obj) => {
        return obj.action === "pageview" && obj.user_id === lead.user_id;
      }).length,
      score_view_video: events.filter((obj) => {
        return (
          obj.action === "click_view_video_luminaire" &&
          obj.user_id === lead.user_id
        );
      }).length,
    };
  });

  // //distinct
  // const key = 'user_id';
  // const dt_result = [...new Map(result.map(item => [item[key], item])).values()];

  const identify_lead = result.filter((item) => {
    return item.email || item.phone;
  });
  console.log(identify_lead.length);

  //distinct
  const key = "user_id";
  const dt_identify_lead = [
    ...new Map(identify_lead.map((item) => [item[key], item])).values(),
  ];

  console.log(dt_identify_lead.length);
  const ndJson = dt_identify_lead.map(JSON.stringify).join("\n");
  await fs.writeFileSync("./json/data.json", ndJson);
  const dsname = "report";
  const dtname = "RawLead";
  // await clearData(dsname, dtname);

  console.log(dsname);
  console.log(dtname);
  const options = {
    sourceFormat: "NEWLINE_DELIMITED_JSON",
    schema: {
      fields: [
        { name: "user_id", type: "STRING" },
        { name: "fullname", type: "STRING" },
        { name: "email", type: "STRING" },
        { name: "phone", type: "STRING" },
        { name: "fb_id", type: "STRING" },
        { name: "google_analytic_id", type: "STRING" },
        { name: "score_hcm", type: "INTEGER" },
        { name: "score_hn", type: "INTEGER" },
        { name: "score_pageview", type: "INTEGER" },
        { name: "score_view_video", type: "INTEGER" },
      ],
    },
    location: "asia-southeast1",
  };
  try {
    const [job] = await bigquery
      .dataset(dsname)
      .table(dtname)
      .load(RawLeadFile, options);
    console.log(`Job ${job.id} completed.`);

    // Check the job's status for errors
    const errors = job.status.errors;
    if (errors && errors.length > 0) {
      throw errors;
    } else {
      res.send(job);
    }
  } catch (err) {
    res.send(err);
  }
});

const server = app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});
