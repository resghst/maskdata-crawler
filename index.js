require('dotenv').config()
const _ = require('lodash')
const axios = require('axios')
const Papa = require('papaparse')
var URL = require('url').URL;
/**
 * 取得 process.env.[key] 的輔助函式，且可以有預設值
 */
const getenv = (key, defaultval) => {
  return _.get(process, ['env', key], defaultval)
}

let getCsv = async url => {
  url = new URL(url)
  const csv = _.trim(_.get(await axios.get(url.href), 'data'))
  return _.get(Papa.parse(csv, {
    encoding: 'utf8',
    header: true,
  }), 'data', [])
}

const CSV_MASK = 'https://data.nhi.gov.tw/resource/mask/maskdata.csv'
exports.getMasks = async () => {
  const masks = await getCsv(CSV_MASK)
  console.log(`取得 ${masks.length} 筆口罩數量資料`)
  const mask = _.fromPairs(_.map(masks, mask => [
    mask['醫事機構代碼'],
    {
      id: mask['醫事機構代碼'],
      adult: _.parseInt(mask['成人口罩剩餘數']),
      child: _.parseInt(mask['兒童口罩剩餘數']),
      mask_updated: mask['來源資料時間'],
    }
  ]))
  return mask
}

const CSV_STORE = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vT4tWc7zUcHQQ8LC_0276aOZNcIBu544YB9XrSRz7oq66q5lE3RHN5Ix2-4S3NL4bL-0zi5nKzE13eX/pub?gid=0&single=true&output=csv'
exports.getStores = async () => {
  const stores = await getCsv(CSV_STORE)
  console.log(`取得 ${stores.length} 筆店家資料`)
  return stores
}

const unparseCsv = (data) => {
  return Papa.unparse(data, {
    header: true
  })
}

exports.uploadCSV = async (mask, maxAge=30)=>{
  const {Storage} = require('@google-cloud/storage');
  const projectId = getenv('projectId')
  const keyFilename = getenv('keyFilename')
  const maskBucket = getenv('maskBucket')
  const maskFile = getenv('maskFile')

  const storage = new Storage({projectId, keyFilename});
  const file = storage.bucket(maskBucket).file(maskFile);
  console.log(mask)
  return await file.save( mask ,{
    gzip: true,
    validation:'crc32c',
    matadata:{
      cacheControl: `public, max-age=${maxAge}`,
      contentType:'text/csv',
      contentLanguage: 'zh',
    },
  })
}


exports.scheduler = async () =>{
  const [masks, stores] = await Promise.all([
    exports.getMasks(), exports.getStores()
  ])
  // console.log(stores)
  _.each(stores,row=>{    
    const mask = _.get(masks, row.id)
    if(!_.isNil(mask)){
      row['adult']=_.parseInt(masks[row.id]['adult'])
      row['child']=_.parseInt(masks[row.id]['child'])
      row['mask_updated']=new Date(masks[row.id]['mask_updated']).getTime()
    }
  })
  return await exports.uploadCSV(unparseCsv(stores))
}


Promise.all([exports.scheduler()])
.catch(onRejected=>console.log(onRejected));

