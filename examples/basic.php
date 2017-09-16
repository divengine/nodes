<?php

/**
 * divNoSQL
 *
 * This is a basic example of divNoSQL that demonstrates their main functionalities.
 *
 * @author Rafa Rodriguez [@rafageist] <rafageist@hotmail.com>
 */
include "../divNodes.php";

// Adding a schema on the fly if not exists
$db = new divNodes("database/contacts");

// Deleting all nodes in schema database/contacts
$db->delNodes();

// Add node into schema database/contacts
$id = $db->addNode([
	"name" => "Peter",
	"age" => 25
]);

// Change data of node
$db->setNode($id, [
	"email" => "peter@email.com",
	"phone" => "+1222553335"
]);

$id = $db->addNode(array(
	"name" => "John",
	"age" => 15
));

// Retrieve a node from schema database/contacts
$contact = $db->getNode($id);

$db->forEachNode(function(&$contact, $id, &$db){

	if ($contact['age'] < 20)
		return DIV_NODES_FOR_CONTINUE_DISCARDING;

	if ( ! isset($contact['visitors']))
		$contact['visitors'] = 0;

	$contact['visitors']++;

	echo "Name: {$contact['name']} <br/>\n";
	echo "Phone: {$contact['phone']} <br/>\n";
	echo "Email: {$contact['email']} <br/>\n";
	echo "Visitors: {$contact['visitors']} <br/>\n";
});

// Remove node
$db->delNode($id);
