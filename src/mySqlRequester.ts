/// <reference path="../typings/mysql/mysql.d.ts" />
/// <reference path="../typings/q/Q.d.ts" />
/// <reference path="../definitions/locator.d.ts" />
/// <reference path="../definitions/requester.d.ts" />
"use strict";

import mysql = require("mysql");
import Q = require('q');

export interface MySqlRequesterParameters {
  locator?: Locator.PlywoodLocator;
  host?: string;
  user: string;
  password: string;
  database: string;
}

function basicLocator(host: string): Locator.PlywoodLocator {
  var hostnamePort = host.split(':');
  var hostname: string;
  var port: number;
  if (hostnamePort.length > 1) {
    hostname = hostnamePort[0];
    port = Number(hostnamePort[1]);
  } else {
    hostname = hostnamePort[0];
    port = 3306;
  }
  return () => {
    return Q({
      hostname: hostname,
      port: port
    });
  };
}

export function mySqlRequesterFactory(parameters: MySqlRequesterParameters): Requester.PlywoodRequester<string> {
  var locator = parameters.locator;
  if (!locator) {
    var host = parameters.host;
    if (!host) throw new Error("must have a `host` or a `locator`");
    locator = basicLocator(host);
  }
  var user = parameters.user;
  var password = parameters.password;
  var database = parameters.database;

  return (request): Q.Promise<any[]> => {
    var query = request.query;
    return locator()
      .then((location) => {
        var connection = mysql.createConnection({
          host: location.hostname,
          port: location.port || 3306,
          user: user,
          password: password,
          database: database,
          charset: 'UTF8_BIN',
          timezone: '+00:00',
        });

        connection.connect();

        var deferred = <Q.Deferred<any[]>>(Q.defer());
        connection.query(query, (err, data) => {
          if (err) {
            deferred.reject(err);
          } else {
            deferred.resolve(data);
          }
        });
        connection.end();
        return deferred.promise;
      });
  };
}
