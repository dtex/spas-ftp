var Connection = require("ssh2"),
	async = require("async"),
	ReadLine = require('readline'),
	stream = require('stream'),
	dummyStream = new stream.Stream(), // ReadLine requires an output stream, but we don't need it.
	delimiter = "\t";

var parseRemoteFile = function (sftp) {
	
	return function (file, onDone) {
		var data = [], keys, cols,
			rl = ReadLine.createInterface({
				input: sftp.createReadStream(file.path, {encoding: 'utf8'}),
				output: dummyStream
			});

		rl.on("line", function (line) {
			cols = line.split(delimiter);
			if (!keys) {
				keys = cols;
			} else {
				var item = {};
				for (var i = 0; i < keys.length; i++) {
					item[keys[i]] = cols[i];
				}
				data.push(item);
			}
		}).on("close", function () {
			onDone(null, data);
		});
	};
};

var deleteRemoteFile = function(sftp) {
	return function (file, onDone) {
		sftp.unlink(file.path, onDone);
	};
};

exports["request"] = function (params, credentials, callback) {
	var c = new Connection(),
		files = params.files;

	if (params.delimiter) delimiter = params.delimiter;

	c.on('ready', function() {
		
		c.sftp(function (err, sftp) {
			
			if (err) return callback(err, null);
		
			// Parse each file asynchronously.
			async.map(files, parseRemoteFile(sftp), function (err, results) {

				// This callback is called after all async operations
				// are done. The results are the array of all file data.
				if (err) return callback(err, null);
				
				// Convert the results to an object.
				var result = results.reduce(function (obj, currentData, index) {
					obj[files[index].id] = currentData;
					return obj;
				}, {});

				if (params.deleteAfter) {
					
					async.each(files, deleteRemoteFile(sftp), function (err) {
						
						// Not sure what to do when error occurs.
						// sftp.end();
					});

				} else {

					// Ends the SFTP session.
					// sftp.end();
				}
				
				callback(null, result);
			});
		});
	}).on("error", function (err) {
		callback(err, null);
	});

	c.connect({
		host: params.host || "localhost",
		username: credentials.username || "anonymous",
		password: credentials.password || ""
	});

};