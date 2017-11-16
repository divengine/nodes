<?php

/**
 * divNoSQL
 *
 * References example
 *
 * @author Rafa Rodriguez [@rafageist] <rafageis@hotmail.com>
 *
 */
include "../divNodes.php";

// Adding a schema on the fly if not exists
$db = new divNodes("database/companies");

// Deleting all nodes in schema database/companies
$db->delNodes();

// Add node into schema database/companies
$id = $db->addNode([
	"name" => "Company A",
	"phone" => "1-(111) 223-333"
]);

// Change data of node
$db->setNode($id, [
	"email" => "questions@example.com",
	"phone" => "+1233455566"
]);

// Retrieve a node from schema database/companies
$company = $db->getNode($id);

echo $company['name'] . " - ";

// Adding a schema on the fly if not exists
$db->addSchema("database/employees");

// Clear the new schema
$db->delNodes([], "database/employees");

// Add reference on the fly
$db->addReference([
	"schema" => "database/employees",
	"foreign_schema" => "database/companies",
	"property" => "company"
]);

// Add node related
$ide = $db->addNode([
	"name" => "Rafa Rodriguez",
	"company" => $id
], null, "database/employees");

// Add another node related
$ide = $db->addNode([
	"name" => "Peter Joseph",
	"company" => $id
], null, "database/employees");

// Retrieve employees
$employees = $db->getNodes([
	"where" => "{company} == '$id'",
	"offset" => 0,
	"limit" => 20,
	"fields" => "name",
	"order" => "name",
	"order_asc" => true
], "database/employees");

foreach($employees as $ide => $employee)
{
	echo $employee['name'] . ", ";
}

// When removing the company, they will also remove the related records
$db->delNode($id);
