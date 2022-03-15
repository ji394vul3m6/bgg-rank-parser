const request = require('request');
const { parse } = require('node-html-parser');
const { MongoClient } = require('mongodb');
const mongoURL = 'mongodb://localhost:27017';

const cacheBoardgame = 5000
const DBName = 'bgg'

function stringToByteArray(s) {
  // Otherwise, fall back to 7-bit ASCII only
  var result = new Uint8Array(s.length);
  for (var i = 0; i < s.length; i++) {
    result[i] = s.charCodeAt(i);/* w ww. ja  v  a 2s . co  m*/
  }
  return result;
}

const getURL = (page) => {
  return `https://boardgamegeek.com/browse/boardgame/page/${page}/`
}

const getWeb = async (uri) => {
  return new Promise((r, rej) => {
    request({
      method: 'GET',
      uri,
      followRedirect: false,
    }, (err, response) => {
      if (err) {
        rej(err);
        return;
      }
      r(response.body);
    });
  })
}

const getPageContent = async (page) => {
  return new Promise((r, rej) => {
    request({
      method: 'GET',
      uri: getURL(page),
      followRedirect: false,
    }, (err, response) => {
      if (err) {
        rej(err);
        return;
      }
      if (response.statusCode >= 500) {
        rej('status 500');
      }
      r(response.body);
    });
  })
}

const getImgSrc = (root, selector) => {
  return root.querySelector(selector)?.getAttribute('src') || '';
}

const getText = (dom) => {
  return dom?.textContent.replace(/[\n \t]/g, '') || '';
}

const getDOMText = (root, selector) => {
  return root.querySelector(selector)?.textContent.replace(/[\n \t]/g, '') || '';
}

const getBGID = (root, selector) => {
  const link = root.querySelector(selector)?.getAttribute('href') || '';
  const regex = /boardgame\/([0-9]+)\//
  // console.log(link, regex);

  const result = link.match(regex);
  if (result && result.length > 1) {
    return result[1];
  }
  return undefined
}

const parseRowData = (row) => {
  const cells = Array.from(row.querySelectorAll('td'));

  const rank = getText(cells[0]);
  const imgSrc = getImgSrc(cells[1], 'img');
  const name = getDOMText(cells[2], 'a');
  const id = getBGID(cells[2], 'a');
  const geekRate = getText(cells[3]);
  const avgRate = getText(cells[4]);
  return {
    id,
    rank,
    imgSrc,
    name,
    geekRate,
    avgRate,
  }
}

const getPageData = async (page) => {
  const content = await getPageContent(page);
  const root = parse(content);

  const table = root.querySelector('#collectionitems');
  const rows = Array.from(table.querySelectorAll('tr'));
  rows.shift() // ignore title row

  return rows.map(parseRowData);
}

const getImageFile = async (url) => {
  return new Promise((r, rej) => {
    const https = require('https');
    https.get(url, function (response) {
      var data = [];

      response
        .on('data', function (chunk) {
          data.push(chunk);
        })
        .on('end', function () {
          var buffer = Buffer.concat(data);
          r(buffer);
        })
        .on('error', e => {
          rej(e);
        });
    });
  })
}

const storeBGData = async (client, bgData) => {
  for (let i = 0; i < bgData.length; i++) {
    const data = bgData[i];
    await storeData(client, data);
  }
};

const storeData = async (client, data) => {
  const collection = client.db(DBName).collection('data');
  const now = new Date();

  const year = now.getFullYear();
  const month = now.getMonth() + 1;
  const day = now.getDate();

  const query = {
    id: data.id,
    year, month, day
  };
  const update = {
    $set: {
      ...data,
      year,
      month,
      day,
    }
  };
  const options = { upsert: true };
  await collection.updateOne(query, update, options);
}

const runTask = async () => {
  const client = new MongoClient(mongoURL);
  await client.connect();
  await client.db(DBName).command({ ping: 1 })

  const delay = (ms) => {
    return new Promise(r => {
      setTimeout(() => { r(); }, ms);
    })
  }

  const maxPage = parseInt((cacheBoardgame - 1) / 100, 10) + 1

  for (let i = 1; i <= maxPage; i++) {
    try {
      console.log(`Load data of page ${i}`)
      const bgData = await getPageData(i)
      if (bgData) {
        console.log(`\tStore data of page ${i}`)
        await storeBGData(client, bgData);
        console.log(`\tStore data of page ${i} finish`)
      }
    } catch (e) {
      console.log(`Get data of page ${i} fail. ${e.message}`);
    }
    delay(100);
  }
  process.exit(0);
}

runTask();
