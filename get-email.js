var mongo = require('mongodb');
var MongoClient = mongo.MongoClient;

var mongoHost = process.env.MONGO_HOST || 'bubbles-shard-00-00-t7bij.mongodb.net:27017,bubbles-shard-00-01-t7bij.mongodb.net:27017,bubbles-shard-00-02-t7bij.mongodb.net:27017/bubbles?ssl=true&replicaSet=Bubbles-shard-0&authSource=admin';
var mongoUser = process.env.MONGO_USER || 'bubbles';
var mongoPass = process.env.MONGO_PASS;

var url = `mongodb://${mongoUser}:${mongoPass}@${mongoHost}`;
var fs = require('fs')
var csvWriter = require('csv-write-stream')

MongoClient.connect(url)
  .then(db => db.collection('users').find().toArray())
  .then(users => {
    var writer = csvWriter({ headers: ['id', 'name', 'email']})
    writer.pipe(fs.createWriteStream('./emails.csv'))

    return users
      .filter(({ email}) => new RegExp('estudiant').test(email))
      .map(({_id, email, name}) => ({id: _id, email, name}))
      .forEach(u => writer.write(u))
    writer.end()
  })
  .catch(err => {
    console.log(err)
  })
