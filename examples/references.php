<?php

/**
 * divNoSQL
 *
 * References example
 *
 * @author Rafa Rodriguez <rafacuba2015@gmail.com>
 *        
 */
include "../../divNoSQL.php";

// Adding a schema on the fly if not exists
$db = new divNoSQL("database/companies");

// Deleting all nodes in schema database/companies
$db->delNodes();

// Add node into schema database/companies
$id = $db->addNode(array(
		"name" => "Company A",
		"phone" => "1-(111) 223-333"
));

// Change data of node
$db->setNode($id, array(
		"email" => "questions@example.com",
		"phone" => "+1233455566"
));

// Retrieve a node from schema database/companies
$company = $db->getNode($id);

echo $company['name'] . " - ";

// Adding a schema on the fly if not exists
$db->addSchema("database/employees");

// Clear the new schema
$db->delNodes(array(), "database/employees");

// Add reference on the fly
$db->addReference(array(
		"schema" => "database/employees",
		"foreign_schema" => "database/companies",
		"property" => "company"
));

// Add node related
$ide = $db->addNode(array(
		"name" => "Rafa Rodriguez",
		"company" => $id
), null, "database/employees");

// Add another node related
$ide = $db->addNode(array(
		"name" => "Peter Joseph",
		"company" => $id
), null, "database/employees");

// Retrieve employees
$employees = $db->getNodes(array(
		"where" => "{company} == '$id'",
		"offset" => 0,
		"limit" => 20,
		"fields" => "name",
		"order" => "name",
		"order_asc" => true
), "database/employees");

foreach ( $employees as $ide => $employee ) {
	echo $employee['name'] . ", ";
}

// When removing the company, they will also remove the related records
$db->delNode($id);
