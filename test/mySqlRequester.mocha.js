var { expect } = require("chai");

var { mySqlRequesterFactory } = require('../build/mySqlRequester');

var info = require('./info');

var mySqlRequester = mySqlRequesterFactory({
  host: info.mySqlHost,
  database: info.mySqlDatabase,
  user: info.mySqlUser,
  password: info.mySqlPassword
});

describe("MySQL requester", function() {
  this.timeout(5 * 1000);

  describe("error", function() {
    it("throws if there is not host or locator", function() {
      expect(() => {
        mySqlRequesterFactory({});
      }).to.throw('must have a `host` or a `locator`');
    });

    it("correct error for bad table", (testComplete) => {
      mySqlRequester({
        query: "SELECT * FROM not_a_real_datasource"
      })
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((err) => {
          expect(err.message).to.contain("ER_NO_SUCH_TABLE");
          testComplete();
        })
        .done();
    });
  });


  describe("basic working", function() {
    it("runs a DESCRIBE", (testComplete) => {
      mySqlRequester({
        query: "DESCRIBE wikipedia;"
      })
        .then((res) => {
          expect(res.length).to.equal(23);
          testComplete();
        })
        .done();
    });


    it("runs a SELECT / GROUP BY", (testComplete) => {
      mySqlRequester({
        query: 'SELECT `channel` AS "Channel", sum(`added`) AS "TotalAdded", sum(`deleted`) AS "TotalDeleted" FROM `wikipedia` WHERE `cityName` = "Tokyo" GROUP BY `channel`;'
      })
        .then((res) => {
          expect(res).to.deep.equal([
            {
              "Channel": "de",
              "TotalAdded": 0,
              "TotalDeleted": 109
            },
            {
              "Channel": "en",
              "TotalAdded": 3500,
              "TotalDeleted": 447
            },
            {
              "Channel": "fr",
              "TotalAdded": 0,
              "TotalDeleted": 0
            },
            {
              "Channel": "ja",
              "TotalAdded": 75168,
              "TotalDeleted": 2462
            },
            {
              "Channel": "ko",
              "TotalAdded": 0,
              "TotalDeleted": 57
            },
            {
              "Channel": "ru",
              "TotalAdded": 898,
              "TotalDeleted": 194
            },
            {
              "Channel": "zh",
              "TotalAdded": 72,
              "TotalDeleted": 21
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works correctly with time", (testComplete) => {
      mySqlRequester({
        query: 'SELECT MAX(`time`) AS "MaxTime" FROM `wikipedia` GROUP BY ""'
      })
        .then((res) => {
          expect(res).to.deep.equal([
            {
              "MaxTime": new Date('2015-09-12T23:59:00.000Z')
            }
          ]);
          testComplete();
        })
        .done();
    })
  });
});
