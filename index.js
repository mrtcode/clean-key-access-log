/*
 ***** BEGIN LICENSE BLOCK *****
 
 This file is part of the Zotero Data Server.
 
 Copyright © 2018 Center for History and New Media
 George Mason University, Fairfax, Virginia, USA
 http://zotero.org
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 ***** END LICENSE BLOCK *****
 */

const mysql2 = require('mysql2');
const mysql2Promise = require('mysql2/promise');
const through2 = require('through2');
const config = require('config');

let mysqlMaster = null;

async function cleanKey(keyID) {
	let numDeleted = 0;
	// There should be at least 6 rows to issue delete operation
	let result = await mysqlMaster.execute(
		'SELECT timestamp FROM keyAccessLog WHERE keyID = ? ORDER BY timestamp DESC LIMIT 6',
		[keyID]);
	
	if (result[0].length >= 6) {
		// 5th row
		let timestamp = result[0][4].timestamp;
		
		let result2 = await mysqlMaster.execute(
			'DELETE FROM keyAccessLog WHERE keyID = ? AND timestamp < ?',
			[keyID, timestamp]
		);
		numDeleted = result2[0].affectedRows;
	}
	return numDeleted;
}

async function cleanKeys() {
	let mysqlMasterStream;
	await new Promise(function (resolve, reject) {
		mysqlMasterStream = mysql2.createConnection(config.get('master'));
		mysqlMasterStream.connect(function (err) {
			if (err) return reject(err);
			
			let keyNr = 0;
			mysqlMasterStream.query('SELECT keyID FROM `keys` ORDER BY keyID')
				.stream({highWaterMark: 1000})
				.pipe(through2({objectMode: true}, async function (row, enc, next) {
					keyNr++;
					
					if (keyNr % 10000 === 0) {
						console.log(keyNr);
					}
					
					// await new Promise(function (resolve) {
					// 	setTimeout(resolve, 1000);
					// });
					
					next();
				}))
				.on('data', function () {
				})
				.on('end', resolve)
				.on('error', reject);
		});
	});
	
	mysqlMasterStream.close();
}

async function main() {
	mysqlMaster = await mysql2Promise.createConnection(config.get('master'));
	await cleanKeys();
	mysqlMaster.close();
}

main();
