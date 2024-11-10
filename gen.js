#!/usr/bin/env node
var Chance = require('chance');
var fs = require('fs');

// Instantiate chance with a seed, so we get the same files each time.
var chance = new Chance('redfi_seed');

function generate(count, filename) {
	var out = '';
	var bytes = '';
	var handle = fs.openSync(filename,'a');

	for (i=0; i<=count; i++) {
   	 out = chance.natural()  + ': {"id":"' +  chance.guid() + '","data":"' + chance.paragraph({ sentences: 5 }) + "\"\}";
   	 bytes = fs.writeSync(handle, out + '\n', null, null);
	};
	fs.closeSync(handle);
	console.log(filename + " write/append completed\n");
}



generate(100,"example_input_data_1.data");
generate(10000,"example_input_data_2.data");
//Chunk last file, as node hangs on large values 
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
generate(10000,"example_input_data_3.data");
