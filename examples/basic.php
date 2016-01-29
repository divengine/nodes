<?php

/**
 * divNoSQL
 *
 * This is a basic example of divNoSQL that demonstrates their main functionalities.
 *
 * @author Rafa Rodriguez <rafacuba2015@gmail.com>
 *        
 */
include "../divNoSQL.php";

// Adding a schema on the fly if not exists
$db = new divNoSQL("database/contacts");

// Deleting all nodes in schema database/contacts
$db->delNodes();

// Add node into schema database/contacts
$id = $db->addNode(array(
		"name" => "Peter",
		"age" => 25
));

// Change data of node
$db->setNode($id, array(
		"email" => "peter@email.com",
		"phone" => "+1222553335"
));

// Retrieve a node from schema database/contacts
$contact = $db->getNode($id);

echo "Name: " . $contact['name'] . " <br/>\n";
echo "Phone: " . $contact['phone'] . " <br/>\n";
echo "Email: " . $contact['email'] . " <br/>\n";

// Remove node

$db->delNode($id);
