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
          expect(res.map(r => {
            return r.Field + ' ~ ' + r.Type;
          })).to.deep.equal([
            "time ~ datetime",
            "sometimeLater ~ timestamp",
            "channel ~ varchar(255)",
            "cityName ~ varchar(255)",
            "comment ~ varchar(300)",
            "commentLength ~ int(11)",
            "countryIsoCode ~ varchar(255)",
            "countryName ~ varchar(255)",
            "deltaBucket100 ~ int(11)",
            "isAnonymous ~ tinyint(1)",
            "isMinor ~ tinyint(1)",
            "isNew ~ tinyint(1)",
            "isRobot ~ tinyint(1)",
            "isUnpatrolled ~ tinyint(1)",
            "metroCode ~ int(11)",
            "namespace ~ varchar(255)",
            "page ~ varchar(255)",
            "regionIsoCode ~ varchar(255)",
            "regionName ~ varchar(255)",
            "user ~ varchar(255)",
            "count ~ bigint(21)",
            "added ~ decimal(32,0)",
            "deleted ~ decimal(32,0)",
            "delta ~ decimal(32,0)",
            "min_delta ~ int(11)",
            "max_delta ~ int(11)",
            "deltaByTen ~ double"
          ]);
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
