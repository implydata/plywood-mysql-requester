var { expect } = require("chai");

var { mySqlRequesterFactory } = require('../build/mySqlRequester');

var info = require('./info');

var mySqlRequester = mySqlRequesterFactory({
  host: info.mySqlHost,
  database: info.mySqlDatabase,
  user: info.mySqlUser,
  password: info.mySqlPassword
});

describe("MySQL requester", function () {
  this.timeout(5 * 1000);

  describe("error", function () {
    it("throws if there is not host or locator", function () {
      expect(function () {
          mySqlRequesterFactory({});
        }
      ).to.throw('must have a `host` or a `locator`');
    });

    it("correct error for bad table", function (testComplete) {
      mySqlRequester({
        query: "SELECT * FROM not_a_real_datasource"
      })
        .then(function () {
          throw new Error('DID_NOT_ERROR');
        })
        .then(null, function (err) {
            expect(err.message).to.contain("ER_NO_SUCH_TABLE");
            return testComplete();
          }
        )
        .done();
    });
  });


  describe("basic working", function () {
    it("runs a DESCRIBE", function (testComplete) {
      mySqlRequester({
        query: "DESCRIBE wikipedia;"
      })
        .then(function (res) {
            expect(res.length).to.equal(23);
            testComplete();
          }
        )
        .done();
    });


    it("runs a SELECT / GROUP BY", function (testComplete) {
      mySqlRequester({
        query: 'SELECT `channel` AS "Channel", sum(`added`) AS "TotalAdded" FROM `wikipedia` GROUP BY `channel`;'
      })
        .then(function (res) {
            expect(res.length).to.equal(52);
            testComplete();
          }
        )
        .done();
    });
  });
});
