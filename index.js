const express = require('express');
//const mongoose = require('mongoose');
//const mongoConnectionUri = "mongodb://localhost:27017/stock_market_db";
const routes = require('./routes/stocks');
const bodyParser = require('body-parser');
const startMetricsServer = require('./metrics/metrics');
const swaggerDocs = require('./utils/swagger');
const jwt = require('jsonwebtoken');
const cors = require('cors');

// mongoose.connect(mongoConnectionUri, () => {
//   console.log("Connected to DB");
// });
// const database = mongoose.connection;

// database.on('error', (error) => {
//   console.log("DB connection error", error);
// })]

const app = express();

app.use(bodyParser.json());

app.get('/', (req, res) => {
  res.json({ status: "I'm alive!" });
})
/**
  * @openapi
  * /healthcheck:
  *  get:
  *     tags:
  *     - Healthcheck
  *     description: Responds if the app is up and running
  *     responses:
  *       200:
  *         description: App is up and running
  */
app.use('/healthcheck', require('express-healthcheck')({
  healthy: function () {
    return { everything: 'is ok' };
  }
}));

app.use(express.json());

app.use(cors({
  origin: '*'
}))
app.use('/stocks', verifyToken, routes);

app.post('/api/login', (req, res) => {
  // Mock user
  const user = req.body;

  jwt.sign({ user }, 'secretkey', { expiresIn: '15000s' }, (err, token) => {
    res.json({
      username: req.body.username,
      password: req.body.password,
      token : token
    });
  });
});


function verifyToken(req, res, next) {
  // Get auth header value
  const bearerHeader = req.headers['authorization'];
  // Check if bearer is undefined
  if (typeof bearerHeader !== 'undefined') {
    // Split at the space
    const bearer = bearerHeader.split(' ');
    // Get token from array
    const bearerToken = bearer[1];
    // Set the token
    req.token = bearerToken;
    // Next middleware
    jwt.verify(bearerToken, 'secretkey', (err, authData) => {
      if (err) {
        res.sendStatus(403);
      } else {
        console.log('AuthData' + JSON.stringify(authData));
        next();
      }
    });

  } else {
    // Forbidden
    res.sendStatus(403);
  }
}
const port = 8080;
app.listen(port, () => {
  console.log(`Server Started at ${port}`)

  startMetricsServer();
  swaggerDocs(app, port);
})




