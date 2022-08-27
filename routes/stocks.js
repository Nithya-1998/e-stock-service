const express = require("express");
const Company = require("../model/Company");
const Router = express.Router();
//const producer = require('../kafka/producer');

let aws = require("aws-sdk");
let isMongoDBEnabled = false;
let awsConfig = {
  region: "us-east-1",
  endpoint: "https://dynamodb.us-east-1.amazonaws.com",
  accessKeyId: "AKIAQOJ5XWDPRWFFHFXX",
  secretAccessKey: "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",
};
aws.config.update(awsConfig);

let dynamodbTableName = "Company";

let dynamodb = new aws.DynamoDB.DocumentClient();

Router.post("/getCompanyStocks", async (req, res, next) => {
  console.log(req.body);
  try {
    const companies = [];
    if (req.body != null && req.body != undefined) {
      if (isMongoDBEnabled) {
        const query = (
          await Company.find({ companyCode: req.body.companyCode }, null, {
            sort: { date: -1 },
          })
        ).forEach((company) => {
          companies.push(company);
        });
        console.log(companies);
        res.status(200).json(companies);
      } else {
        if (req.body != null && req.body != undefined) {
          const params = {
            TableName: dynamodbTableName,
          };
          try {
            const companies = await scanDynamoRecords(params, []);
            const body = {
              companies: companies,
            };
            console.log("All Companies: ", companies);
            res.status(200).json(companies);
          } catch (error) {
            console.error(
              "Do your custom error handling here. I am just ganna log it out: ",
              error
            );
            res.status(500).json({ err: error });
          }
        }
      }
    }
  } catch (error) {
    next(error);
    res.status(500).json({ err: error });
  }
});

Router.get("/getCompany/:companyCode", async (req, res, next) => {
  try {
    if (req.params != null && req.params != undefined) {
      if (isMongoDBEnabled) {
        console.log(req.params.companyCode);
        let companies = [];
        const query = Company.findOne(
          { companyCode: req.params.companyCode },
          null,
          (err, company) => {
            if (err) {
              console.log(err);
            }
            console.log("Find one: " + company);
            res.status(200).json({ data: company });
          }
        );
      } else {
        if (req.body != null && req.body != undefined) {
          let params = {
            TableName: dynamodbTableName,
            Key: {
              companyCode: req.params.companyCode,
            },
          };
          dynamodb.get(params, function (err, data) {
            if (err) {
              console.log(JSON.stringify(err));
            } else {
              console.log("Find one: " + data.Item);
              res.status(200).json({ data: data.Item });
            }
          });
        }
      }
    }
  } catch (error) {
    console.log(error);
  }
});

/**
 * @openapi
 * '/stocks/getAllCompanyStocks':
 *  post:
 *     tags:
 *     - Get all Companies
 *     summary: All company details
 *     requestBody:
 *       required: true
 *       content:
 *          application/json:
 *            schema:
 *              type: object
 *
 *     responses:
 *       200:
 *         description: Success
 *         content:
 *          application/json:
 *            schema:
 *              type: array
 *              items:
 *                type: object
 *                properties:
 *                  companyCode:
 *                    type: string
 *                  companyName:
 *                    type: string
 *                  date:
 *                    type: string
 *                    format: date-time
 *                  logo:
 *                    type: string
 *                  stockPriceHigh:
 *                    type: string
 *                  stockPriceLow:
 *                    type: string
 *                  currentStockPrice:
 *                    type: string
 *                  volume:
 *                    type: string
 *                  marketcap:
 *                    type: string
 *                  emailId:
 *                    type: string
 *                  companyCEO:
 *                    type: string
 *                  companyWebSite:
 *                    type: string
 *                  stockExchange:
 *                    type: string
 *       403:
 *         description: Unauthorized/Forbidden
 *       400:
 *         description: Bad request
 */
Router.post("/getAllCompanyStocks", async (req, res, next) => {
  console.log(req.body);
  try {
    const companies = [];
    if (isMongoDBEnabled) {
      const query = (
        await Company.find({}, null, { sort: { date: -1 } })
      ).forEach((company) => {
        companies.push(company);
      });
    } else {
      const params = {
        TableName: dynamodbTableName,
      };
      try {
        const companies = await scanDynamoRecords(params, []);
        const body = {
          companies: companies,
        };
        console.log("All Companies: ", companies);
        res.status(200).json(companies);
      } catch (error) {
        console.error("Internal error ", error);
        res.status(500).json({ err: error });
      }
    }
  } catch (error) {
    next(error);
    res.status(500).json({ err: error });
  }
});

/**
 * @openapi
 * '/stocks/addCompany':
 *  post:
 *     tags:
 *     - Add Company
 *     summary: Add/Edit Company
 *     requestBody:
 *       required: true
 *       content:
 *          application/json:
 *            schema:
 *              type: object
 *              properties:
 *                  companyCode:
 *                    type: string
 *                  companyName:
 *                    type: string
 *                  date:
 *                    type: string
 *                    format: date-time
 *                  logo:
 *                    type: string
 *                  stockPriceHigh:
 *                    type: string
 *                  stockPriceLow:
 *                    type: string
 *                  currentStockPrice:
 *                    type: string
 *                  emailId:
 *                    type: string
 *                  companyCEO:
 *                    type: string
 *                  companyWebSite:
 *                    type: string
 *                  stockExchange:
 *                    type: string
 *                  volume:
 *                    type: string
 *                  marketcap:
 *                    type: string
 *     responses:
 *       200:
 *         description: Success
 *         content:
 *          application/json:
 *            schema:
 *              type: object
 *              items:
 *                type: object
 *                properties:
 *                  companyCode:
 *                    type: string
 *                  companyName:
 *                    type: string
 *                  date:
 *                    type: string
 *                    format: date-time
 *                  logo:
 *                    type: string
 *                  stockPriceHigh:
 *                    type: string
 *                  stockPriceLow:
 *                    type: string
 *                  currentStockPrice:
 *                    type: string
 *                  emailId:
 *                    type: string
 *                  companyCEO:
 *                    type: string
 *                  companyWebSite:
 *                    type: string
 *                  stockExchange:
 *                    type: string
 *                  volume:
 *                    type: string
 *                  marketcap:
 *                    type: string
 *       403
 *         description: Unauthorized/ForBidden
 *       500:
 *         description: Internal Server Error
 */
Router.post("/addCompany", async (req, res, next) => {
  try {
    if (isMongoDBEnabled) {
      if (req.body != null && req.body != undefined) {
        console.log("Add Company Stocks " + JSON.stringify(req.body));
        /*To post message to kafka
             producer(req.body.stockPrice).catch((err) => {
                 console.error("error in producer: ", err)
             }) */
        const update = {
          companyCode: req.body.companyCode,
          companyName: req.body.companyName,
          date:
            req.body.date == null ||
            req.body.date == "" ||
            req.body.date == undefined
              ? Date.now()
              : req.body.date,
          stockPriceHigh: req.body.stockPriceHigh,
          stockPriceLow: req.body.stockPriceLow,
          currentStockPrice: req.body.currentStockPrice,
          logo: req.body.logo,
          emailId: req.body.emailId,
          companyCEO: req.body.companyCEO,
          stockExchange: req.body.stockExchange,
          companyWebSite: req.body.companyWebSite,
          volume: req.body.volume,
          marketcap: req.body.marketcap,
        };
        const filter = { companyCode: req.body.companyCode };
        let doc = await Company.findOneAndUpdate(filter, update, {
          new: true,
          upsert: true,
        });
        res.send({
          message: "Post stocks of the company",
        });
      }
    } else {
      console.log("Add New company:");
      const addUser = {
        companyCode: req.body.companyCode,
        companyName: req.body.companyName,
        date:
          req.body.date == null ||
          req.body.date == "" ||
          req.body.date == undefined
            ? Date.now()
            : req.body.date,
        stockPriceHigh: req.body.stockPriceHigh,
        stockPriceLow: req.body.stockPriceLow,
        currentStockPrice: req.body.currentStockPrice,
        logo: req.body.logo,
        emailId: req.body.emailId,
        companyCEO: req.body.companyCEO,
        stockExchange: req.body.stockExchange,
        companyWebSite: req.body.companyWebSite,
        volume: req.body.volume,
        marketcap: req.body.marketcap,
      };
      const params = {
        TableName: dynamodbTableName,
        Item: addUser,
      };
      console.log("Add New Company:", params);
      await dynamodb
        .put(params)
        .promise()
        .then(
          (res) => {
            const body = {
              Operation: "SAVE",
              Message: "SUCCESS",
              Item: addUser,
            };
            console.log("Post Company Details : ", res);
          },
          (error) => {
            console.log("Already exists :", error);
            res.send("User Already Exists");
          }
        );
      res.send({ message: "User Details updated sucessfully..." });
    }
  } catch (error) {
    next(error);
  }
});

/**
 * @openapi
 * '/stocks/deleteCompany/{companyCode}':
 *  delete:
 *     tags:
 *     - Delete Company
 *     summary: Delete Company
 *     requestBody:
 *       required: true
 *       content:
 *          application/json:
 *            schema:
 *              type: object
 *              properties:
 *                  companyCode:
 *                    type: string
 *                  companyName:
 *                    type: string
 *                  date:
 *                    type: string
 *                    format: date-time
 *                  logo:
 *                    type: string
 *                  stockPriceHigh:
 *                    type: string
 *                  stockPriceLow:
 *                    type: string
 *                  currentStockPrice:
 *                    type: string
 *                  emailId:
 *                    type: string
 *                  companyCEO:
 *                    type: string
 *                  companyWebSite:
 *                    type: string
 *                  stockExchange:
 *                    type: string
 *                  volume:
 *                    type: string
 *                  marketcap:
 *                    type: string
 *     responses:
 *       200:
 *         description: Success
 *         content:
 *          application/json:
 *            schema:
 *              type: object
 *              items:
 *                type: object
 *                properties:
 *                  message:
 *                    type: string
 *       403
 *         description: Unauthorized/ForBidden
 *       500:
 *         description: Internal Server Error
 *       404:
 *         description: Not Found
 */
Router.delete("/deleteCompany/:companyCode", async (req, res, next) => {
  try {
    if (req.params != null && req.params != undefined) {
      if (isMongoDBEnabled) {
        await Company.findOneAndDelete({ companyCode: req.params.companyCode });
        console.log("Deleted Successfully");
        res.status(200).json({ message: "Deleted Successfully..." });
      } else {
        const params = {
          TableName: dynamodbTableName,
          Key: {
            companyCode: req.params.companyCode,
          },
          ReturnValues: "ALL_OLD",
        };
        await dynamodb
          .delete(params)
          .promise()
          .then(
            (response) => {
              const body = {
                Operation: "DELETE",
                Message: "SUCCESS",
                Item: response,
              };
              console.log("Deleted Successfully");
              res.status(200).json({ message: "Deleted Successfully..." });
            },
            (error) => {
              console.error("Internal Server Error: ", error);
              res.status(500).send(error);
            }
          );
      }
    }
  } catch (error) {
    next(error);
  }
});

async function scanDynamoRecords(scanParams, itemArray) {
  try {
    const dynamoData = await dynamodb.scan(scanParams).promise();
    itemArray = itemArray.concat(dynamoData.Items);
    if (dynamoData.LastEvaluatedKey) {
      scanParams.ExclusiveStartKey = dynamoData.LastEvaluatedKey;
      return await scanDynamoRecords(scanParams, itemArray);
    }
    return itemArray;
  } catch (error) {
    throw new Error(error);
  }
}

module.exports = Router;
