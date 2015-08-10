{ expect } = require("chai")

{ mySqlRequesterFactory } = require('../build/mySqlRequester')

info = require('./info')

mySqlRequester = mySqlRequesterFactory({
  host: info.mySqlHost
  database: info.mySqlDatabase
  user: info.mySqlUser
  password: info.mySqlPassword
})

describe "MySQL requester", ->
  @timeout(5 * 1000)

  describe "error", ->
    it "throws if there is not host or locator", ->
      expect(->
        mySqlRequesterFactory({})
      ).to.throw('must have a `host` or a `locator`')


    it "correct error for bad table", (testComplete) ->
      mySqlRequester({
        query: "SELECT * FROM not_a_real_datasource"
      })
      .then(-> throw new Error('DID_NOT_ERROR'))
      .then(null, (err) ->
        expect(err.message).to.contain("ER_NO_SUCH_TABLE")
        testComplete()
      )
      .done()


  describe "basic working", ->
    it "runs a DESCRIBE", (testComplete) ->
      mySqlRequester({
        query: "DESCRIBE diamonds;"
      })
      .then((res) ->
        expect(res.length).to.equal(11)
        testComplete()
      )
      .done()


    it "runs a SELECT / GROUP BY", (testComplete) ->
      mySqlRequester({
        query: 'SELECT `cut` AS "Cut", sum(`price`) AS "TotalPrice" FROM `diamonds` GROUP BY `cut`;'
      })
      .then((res) ->
        expect(res.length).to.equal(5)
        testComplete()
      )
      .done()
