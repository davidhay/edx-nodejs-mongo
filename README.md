# edx-nodejs-mongo
Migrate Customer Data to MongoDB

To run the application 

$ npm install 

$ node migrate-data.js [N] 

where N is the number of customer records per parallel job, so 1000 would mean a single job, 1 would mean 1000 parallel jobs. N defaults to 5.

The database used is called 'edx-node-ass3-customers'. 

The collection used is called 'customers';

Before starting, it removes all documents from the 'customers' collection.

After completing, it checks that there are 1000 records in the 'customers' collection.

