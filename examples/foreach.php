<?php

/**
 * Div PHP Nodes
 *
 * Example
 *
 * @author Rafa Rodriguez [@rafageist] <rafageist@hotmail.com>
 */
include "../divNodes.php";

// Adding a schema on the fly if not exists
$db = new divNodes("database/contacts");

// Iterate each node with age < 20
$db->forEachNode(function(&$contact, $id, $schema, $db){

	if ($contact['age'] < 20)
		return DIV_NODES_FOR_CONTINUE_DISCARDING;

	if ( ! isset($contact['visitors']))
		$contact['visitors'] = 0;

	$contact['visitors']++;

	echo "Name: {$contact['name']} <br/>\n";
	echo "Phone: {$contact['phone']} <br/>\n";
	echo "Email: {$contact['email']} <br/>\n";
	echo "Visitors: {$contact['visitors']} <br/>\n";
	echo "\n";
});

