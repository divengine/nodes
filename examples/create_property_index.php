<?php

/**
 * Div PHP Nodes
 *
 * Example
 *
 * @author Rafa Rodriguez [@rafageist] <rafageist@hotmail.com>
 */

include_once "../divNodes.php";

// Adding a schema on the fly if not exists
$db = new divNodes("database/contacts");

// helper function
function random_name()
{
	$word     = '';
	$cons     = 'bcdfghjklmnpqrstvwxyz';
	$cons_len = strlen($cons);

	$vocals     = 'aeiou';
	$vocals_len = strlen($vocals);

	$len = rand(1, 4);
	for($i = 0; $i < $len; $i ++) $word .= $cons[ rand(0, $cons_len - 1) ] . $vocals[ rand(0, $vocals_len - 1) ];

	return ucfirst($word);
}

// Enum of cities
$cities = [
	'NY' => 'New York',
	'FL' => 'Florida',
	'HV' => 'Havana'
];

// Deleting all nodes in schema database/contacts
echo "Clear all CONTACTS ... \n";
$db->delNodes();

// Populate persons for each city
foreach($cities as $city_code => $city_name)
{
	echo "Populating CONTACTS for $city_name\n";

	for($i = 0; $i <= 100; $i ++)
	{
		if(($i + 1) % 10 == 0) echo $i . " ... ";

		$db->addNode([
			'name' => random_name() . ' ' . random_name(),
			'age' => rand(19, 100),
			'city' => $city_code
		]);
	}
	echo "\n";
}

// Create index of object's property
echo "Creating index for CITY property of CONTACTS ...\n";
$idxCity = 'database/index/contacts/city';
$db->delSchema($idxCity);
$db->createIndex(function($node)
{
	echo $node['name'] . "\n";

	return $node['city'];
}, null, $idxCity);

// Search people LIMIT = 10
foreach($cities as $city_code => $city_name)
{
	$people = $db->search($city_code, $idxCity, 0, 10);
	echo "$city_name \n";

	foreach($people as $entry)
	{
		$node = $db->getNode($entry['id'], $entry['schema']);
		echo " - " . $node['name'] . "\n";
	}
}
