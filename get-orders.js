
var mongo = require('mongodb');
var MongoClient = mongo.MongoClient;

var mongoHost = process.env.MONGO_HOST || 'bubbles-shard-00-00-t7bij.mongodb.net:27017,bubbles-shard-00-01-t7bij.mongodb.net:27017,bubbles-shard-00-02-t7bij.mongodb.net:27017/bubbles?ssl=true&replicaSet=Bubbles-shard-0&authSource=admin';
var mongoUser = process.env.MONGO_USER || 'bubbles';
var mongoPass = process.env.MONGO_PASS;

var url = `mongodb://${mongoUser}:${mongoPass}@${mongoHost}`;


var fs = require('fs')
var csvWriter = require('csv-write-stream')
var calcs = require('./calcs');
var _ = require('lodash');

let db;
let users;
let gameLookup;
let gameIds;

MongoClient.connect(url)
  .then(_db => {
    db = _db


    return db.collection('users').find().toArray().then(users => {

    return _(users)
      .filter(({ email}) => new RegExp('estudiant').test(email))
      .filter(({ games }) => games.length > 0)
      .map(({games}) => games.map((g,i) => {
        const round = i%2 == 0 ? 1 : 2;
        return [g._id.toString(), round]
      }))
      .flatten()
      .value()

  })
})
.then(_games => {
  gameLookup = new Map(_games);
  gameIds = new Set(gameLookup.keys());
})
  .then(() => db.collection('orders').find().sort({ timestamp: -1 }).toArray())
  .then(orders => {
    return calcs.getCleared(orders.filter(o => gameIds.has(o.game._id.toString())))
      .map(([a,b]) => {
        return {
          price: calcs.getPrice([a,b]),
          timestamp: calcs.getTime([a,b]),
          user: a.user,
          game: a.game._id,
          round: gameLookup.get(a.game._id),
          period: a.round,
          treated: a.game.treated
        }
      })
  })
  .then(orders => {
    var writer = csvWriter({ headers: ['user', 'game', 'round', 'period', 'price', 'treated', 'timestamp']})

    writer.pipe(fs.createWriteStream('./orders.csv'))

    orders.forEach(o => writer.write(o))
    writer.end()
  })
  .catch(err => {
    console.log(err)
  })
