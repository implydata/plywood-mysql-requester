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

import * as mysql from 'mysql';
import { PlywoodRequester, PlywoodLocator, basicLocator } from 'plywood-base-api';
import { Readable } from 'readable-stream';

export interface MySqlRequesterParameters {
  locator?: PlywoodLocator;
  host?: string;
  user: string;
  password: string;
  database: string;
}

export function mySqlRequesterFactory(parameters: MySqlRequesterParameters): PlywoodRequester<string> {
  let locator = parameters.locator;
  if (!locator) {
    let host = parameters.host;
    if (!host) throw new Error("must have a `host` or a `locator`");
    locator = basicLocator(host, 3306);
  }
  let user = parameters.user;
  let password = parameters.password;
  let database = parameters.database;

  return (request): Readable => {
    let query = request.query;
    let connection: mysql.IConnection = null;

    // options = options || {};
    // options.objectMode = true;
    let stream = new Readable({
      objectMode: true,
      read: function() {
        connection && connection.resume();
      }
    });

    locator()
      .then((location) => {
        connection = mysql.createConnection({
          host: location.hostname,
          port: location.port || 3306,
          user: user,
          password: password,
          database: database,
          charset: 'UTF8_BIN',
          timezone: '+00:00'
        });

        connection.connect();

        let q = connection.query(query);

        q.on('result', function(row) {
          if (!stream.push(row)) connection.pause();
        });

        q.on('error', function(err) {
          stream.emit('error', err);  // Pass on any errors
        });

        q.on('end', function() {
          stream.push(null);  // pushing null, indicating EOF
        });

        connection.end();
      })
      .catch((err: Error) => {
        stream.emit('error', err);  // Pass on any errors
      });

    return stream;
  };
}
