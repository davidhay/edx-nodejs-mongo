const fs = require('fs');

const {MongoClient} = require('mongodb');
const async = require('async');
const assert = require('assert');

const url = 'mongodb://localhost:27017/edx-node-ass3-customers';

const processError = (err,msg) => {
  if(err){
    console.log(`${msg} ${err}`);
    process.exit(-1);
  }
}

const processData = (db, dbCustomers, chunkSize, customers, customerAddresses) => {
  const stop = (msg) => {
    db.close();
    console.log(msg);
    process.exit(-1);
  }
  const createJob = (job) => {
      return (callback) => {
        const start = (job*chunkSize);
        const end = start + (chunkSize-1)
        const msg = `job[${job}] start[${start}] end[${end}]`;
        console.log(msg);

        for(let idx=start;idx<end;idx++){
          Object.assign(customers[idx],customerAddresses[idx]);
          //console.log(`job [${job}][${idx}] ${JSON.stringify(customers[idx])}`);
        }
        const dataToInsert = customers.slice(start,end+1);
        dbCustomers.insert(dataToInsert, (error, results) => {
          callback(error, msg);
        });
      };
  };
  if(!Array.isArray(customers)){
      stop('customers is not an array')
  }
  if(!Array.isArray(customerAddresses)){
    stop('customer addresses is not an array');
  }
  if( customers.length != customerAddresses.length ){
    stop(`customer.length ${customers.length} does not equal customerAddresses.lenfth ${customerAddresses.length}`)
  }
  console.log(`${customers.length} records to process.`);

  var jobs = [];
  const numberJobs = customers.length / chunkSize;
  console.log(`number jobs ${numberJobs}`);

  for(let job=0;job<numberJobs;job++) {
    console.log(`creating job ${job}`);
    jobs.push(createJob(job,chunkSize));
  }

  async.parallel(jobs, (error, results) => {
    processError(error, 'problem running parallel jobs');
    console.log(results);
    dbCustomers.count(function(err, count) {
      processError(err, 'problem counting records');
      assert.equal(customers.length, count);
      db.close();
      console.log('FIN.');
    });//count
  });//parallel
};

const readFile = (filename, callback) => {
  fs.readFile(filename, 'utf8', function (err, rawData) {
      if (err) {
        console.log(`error reading file ${filename} ${err}`);
        process.exit(-1);
      }
      var jsonData = JSON.parse(rawData);
      callback(jsonData);
  });
};

const chunkSize = process.argv[2] || 5;
console.log(`chunk size is ${chunkSize}`)

MongoClient.connect(url, (err,db) => {
  processError(err, 'problem connecting to database');
  const dbCustomers = db.collection('customers');
  dbCustomers.remove({},(err,res) => {
    processError(err, 'problem removing \'customers\' collection');

    readFile('./m3-customer-data.json', (customerData) => {
      readFile('./m3-customer-address-data.json', (customerAddressData) => {
          processData(db, dbCustomers, chunkSize, customerData, customerAddressData);
      });
    });

  });//remove
});//connect
