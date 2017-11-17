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
	$word       = '';
	$cons       = 'bcdfghjklmnpqrstvwxyz';
	$cons_len   = strlen($cons);
	$vocals     = 'aeiou';
	$vocals_len = strlen($vocals);
	$len        = rand(1, 4);
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
die();
// Populate persons for each city
echo "Populating database with random contacts\n";
$stats = [];
$total = rand(10, 10);
for($i = 0; $i < $total; $i ++)
{
	$city_code = array_rand($cities, 1);
	$city_name = $cities[ $city_code ];
	if( ! isset($stats[ $city_code ])) $stats[ $city_code ] = 0;
	$stats[ $city_code ] ++;

	if(($i + 1) % 10 == 0) echo ($i + 1) . " ... ";

	$db->addNode([
		'name' => random_name() . ' ' . random_name(),
		'age' => rand(19, 100),
		'city' => $city_code
	]);
}
echo "\n\n";

// Show stats
echo "Populate results:\n";
foreach($cities as $city_code => $city_name) echo " - $city_name ($city_code) = {$stats[$city_code]} contacts \n";
echo "-------------------------\n";
echo "TOTAL = $total \n\n";

// Create index of object's property
echo "Creating index for CITY property of CONTACTS ...\n";

$idxCity = 'database/index/contacts/city';
$db->delSchema($idxCity);
$db->createIndex(function($node) { return $node['city']; }, null, $idxCity, true);

$db->forEachNode(function($node, $id, $schema, $db, $otherData)
{
	echo "Order for CITY = {$node['name']} ID = $id \n";
	$db->addOrder($node['name'], $id, 'name');

	return DIV_NODES_FOR_CONTINUE_DISCARDING;
});

/*
$db->foreachOrder('name', function($order, $nodeId){
	global $db;
	$node = $db->getNode($nodeId);
	echo $node['name']."\n";
});*/