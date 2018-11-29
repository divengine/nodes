<?php

include "../divNodes.php";

$db = new divNodes("database/lab");

$db->delNodes();
$db->delSchema("/database/lab/index");

$rows = [
    [
        'name' => 'Pepe',
        'age' => 30
    ],
    [
        'name' => 'Juan',
        'age' => 30
    ],
    [
        'name' => 'Maria',
        'age' => 45
    ]
];

foreach ($rows as $row) {
    $id = $db->addNode($row);

    foreach ($row as $prop => $value) {
        $md5 = md5($value);
        $index = new divNodes("database/lab/index/$prop/$md5");

        $idx = $index->getNode($value, null, [
            'schema' => $db->schema,
            'update' => date("Y-m-d h:i:s")
        ]);

        $index->addNode($idx, $id);
    }
}

// filter

$filter = 45;
$prop = 'age';

$md5 = md5($filter);

$data = [
    'db' => &$db,
    'nodes' => []
];

$index = new divNodes("database/lab/index/$prop/$md5");
$index->foreachNode(function ($node, $file, $schema, $db, &$otherData, $iterator) {
    $otherData['nodes'][] = $otherData['db']->getNode($file);
}, null, $data);

var_dump($data['nodes']);

