const mongoose = require('mongoose');

const Company = mongoose.Schema({
    companyCode:{
        type: String,
        required: true
    } ,
    companyName: {
        type: String,
        required: true
    } ,
    date: {
        type: Date,
        default: Date.UTC
    },
    logo: {
        type: String,
        required: false
    },
    stockPriceHigh: {
        type: String,
        required: false
    },
    stockPriceLow: {
        type: String,
        required: false
    },
    emailId: {
        type: String,
        required: true
    },
    stockExchange: {
        type: String,
        required: true
    },
    companyCEO: {
        type: String,
        required: true
    },
    companyWebSite:{
        type: String,
        required: true
    },
    currentStockPrice: {
        type: String,
        required: true
    },
    volume: {
        type: String,
        required: true
    },
    marketcap : {
        type: String,
        required: true
    }

});

module.exports = mongoose.model('Company', Company, 'Company');