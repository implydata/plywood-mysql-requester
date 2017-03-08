/*
 * Copyright 2015-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { expect } = require("chai");
const toArray = require("stream-to-array");

const { mySqlRequesterFactory } = require('../build/mySqlRequester');

const info = require('./info');


let mySqlRequester = mySqlRequesterFactory({
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
      let stream = mySqlRequester({
        query: "SELECT * FROM not_a_real_datasource"
      });

      stream.on('error', (err) => {
        expect(err.message).to.contain("ER_NO_SUCH_TABLE");
        testComplete();
      });
    });

    it("correct error ER_PARSE_ERROR", (testComplete) => {
      let stream = mySqlRequester({
        query: 'SELECT `channel` AS "Channel", sum(`added` AS "TotalAdded" FROM `wikipedia` WHERE `cityName` = "Tokyo" GROUP BY `channel`;'
      });

      stream.on('error', (err) => {
        expect(err.message).to.contain("ER_PARSE_ERROR");
        testComplete();
      });
    });

  });


  describe("basic working", function() {
    it("runs a DESCRIBE", () => {
      let stream = mySqlRequester({
        query: "DESCRIBE wikipedia;"
      });

      return toArray(stream)
        .then((res) => {
          expect(res.map(r => {
            return r.Field + ' ~ ' + r.Type;
          })).to.deep.equal([
            "__time ~ datetime",
            "sometimeLater ~ timestamp",
            "channel ~ varchar(255)",
            "cityName ~ varchar(255)",
            "comment ~ varchar(300)",
            "commentLength ~ int(11)",
            "commentLengthStr ~ varchar(10)",
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
        });
    });


    it("runs a SELECT / GROUP BY", () => {
      let stream = mySqlRequester({
        query: 'SELECT `channel` AS "Channel", sum(`added`) AS "TotalAdded", sum(`deleted`) AS "TotalDeleted" FROM `wikipedia` WHERE `cityName` = "Tokyo" GROUP BY `channel`;'
      });

      return toArray(stream)
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
        });
    });

    it("works correctly with time", () => {
      let stream = mySqlRequester({
        query: 'SELECT MAX(`__time`) AS "MaxTime" FROM `wikipedia` GROUP BY ""'
      });

      return toArray(stream)
        .then((res) => {
          expect(res).to.deep.equal([
            {
              "MaxTime": new Date('2015-09-12T23:59:00.000Z')
            }
          ]);
        });
    })
  });
});
